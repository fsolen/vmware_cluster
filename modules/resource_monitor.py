from pyVmomi import vim
import time
import logging

logger = logging.getLogger('fdrs')

class ResourceMonitor:
    """
    Monitor resources (CPU, Memory, Disk I/O, Network I/O) of VMs and Hosts
    """

    def __init__(self, service_instance):
        self.service_instance = service_instance
        self.performance_manager = service_instance.content.perfManager
        self.counter_map = self._build_counter_map()

    def _build_counter_map(self):
        """
        Builds a map of performance counter names to IDs.
        """
        counter_map = {}
        perf_dict = {}
        perfList = self.performance_manager.perfCounter
        for counter in perfList:
            perf_dict[counter.groupInfo.key + "." + counter.nameInfo.key] = counter.key
        counter_map['cpu.usage'] = perf_dict.get('cpu.usage')
        counter_map['mem.usage'] = perf_dict.get('mem.usage')
        counter_map['disk.usage'] = perf_dict.get('disk.usage')
        counter_map['net.usage'] = perf_dict.get('net.usage')
        return counter_map

    def _get_performance_data(self, entity, metric_name, interval=20):
        content = self.service_instance.RetrieveContent() # This might be inefficient to call repeatedly. Consider if it can be called once per ResourceMonitor instance or less frequently. (Out of scope for this immediate fix)
        metric_id = self.counter_map.get(metric_name)

        # --- START DIAGNOSTIC CODE ---
        entity_name_for_log = getattr(entity, 'name', str(entity)) # Get name if available, else string form
        logger.debug(f"[_get_performance_data] Processing entity: {entity_name_for_log}, Type: {type(entity)}, For Metric: {metric_name}")

        if isinstance(entity, str):
            logger.error(f"[_get_performance_data] CRITICAL: Entity for metric '{metric_name}' is a STRING: '{entity}'. This will cause _moId error.")
            # Depending on desired behavior, either raise an exception or return a default value.
            # Returning a default to see if other entities are processed.
            return 0 # Or None, consistent with other early returns for errors

        if not hasattr(entity, '_moId'):
            logger.error(f"[_get_performance_data] CRITICAL: Entity '{entity_name_for_log}' of type {type(entity)} does not have _moId attribute. Metric: {metric_name}")
            # Returning a default
            return 0 # Or None
        
        logger.debug(f"[_get_performance_data] Entity '{entity_name_for_log}' appears to be a valid managed object with _moId: {entity._moId}")
        # --- END DIAGNOSTIC CODE ---

        if not metric_id:
            logger.warning(f"Metric ID for {metric_name} not found in counter map for entity {entity_name_for_log}!")
            return 0 # Return 0 to match behavior of other error paths in this func

        try:
            query_results = self.performance_manager.QueryPerf(
                querySpec=[
                    vim.PerformanceManager.QuerySpec(
                        entity=entity,
                        metricId=[vim.PerformanceManager.MetricId(counterId=metric_id, instance='')],
                        intervalId=interval,
                        maxSample=1
                    )
                ]
            )

            if query_results and len(query_results) > 0:
                metric_series_list = query_results[0].value # This is a list of MetricSeries objects
                if metric_series_list and len(metric_series_list) > 0:
                    metric_series = metric_series_list[0] # First MetricSeries
                    if hasattr(metric_series, 'value') and metric_series.value and len(metric_series.value) > 0:
                        # metric_series.value is the list of actual data points (e.g., integers for IntSeries)
                        scalar_value = metric_series.value[0]
                        if scalar_value is None: # Handle if the specific sample value is None
                            logger.warning(f"Metric {metric_name} for {entity.name if hasattr(entity, 'name') else 'entity'} has a None value in its series.")
                            return 0 # Default to 0 if specific sample value is None
                        return scalar_value # Return the scalar value
                    else:
                        logger.warning(f"Metric {metric_name} for {entity.name if hasattr(entity, 'name') else 'entity'} has empty or missing 'value' list in its series.")
                else:
                    logger.warning(f"No metric series list found for {metric_name} on {entity.name if hasattr(entity, 'name') else 'entity'}.")
            else:
                logger.info(f"No performance data returned for {metric_name} on {entity.name if hasattr(entity, 'name') else 'entity'}. This might be normal if the counter is not applicable or data is not yet available.")
            return 0 # Default to 0 if no data found or any other issue
        except Exception as e:
            # Log specifics about the entity if possible
            entity_name = getattr(entity, 'name', 'unknown entity')
            logger.error(f"Error fetching scalar performance data for {metric_name} on {entity_name}: {e}")
            return 0 # Default to 0 on error

    def get_vm_metrics(self, vm):
        vm_metrics = {}
        metrics_to_fetch = { # Renamed 'metrics' to 'metrics_to_fetch' to avoid confusion
            "cpu_usage": "cpu.usage",       # Percentage 0-10000
            "memory_usage": "mem.usage",    # Percentage 0-10000
            "disk_io_usage": "disk.usage",  # Assuming KBps (e.g. from a counter like disk.read/write aggregated)
            "network_io_usage": "net.usage" # Assuming KBps (e.g. from a counter like net.tx/rx aggregated)
        }

        for metric_key, counter_key in metrics_to_fetch.items():
            scalar_metric_value = self._get_performance_data(vm, counter_key)

            if scalar_metric_value is None: # Should not happen if _get_performance_data defaults to 0
                scalar_metric_value = 0.0

            if metric_key == "cpu_usage":      # Counter value is 0-10000 (e.g., 5000 means 50%)
                vm_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "memory_usage": # Counter value is 0-10000 (e.g., 5000 means 50%)
                vm_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "disk_io_usage": # Assuming result from counter is in KBps
                vm_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            elif metric_key == "network_io_usage": # Assuming result from counter is in KBps
                vm_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            else:
                vm_metrics[metric_key] = scalar_metric_value # Should not be reached with current keys

        return vm_metrics

    def get_host_metrics(self, host):
        host_metrics = {}
        metrics_to_fetch = {
            "cpu_usage": "cpu.usage",       # Percentage 0-10000
            "memory_usage": "mem.usage",    # Percentage 0-10000
            "disk_io_usage": "disk.usage",  # Assuming KBps
            "network_io_usage": "net.usage" # Assuming KBps
        }

        for metric_key, counter_key in metrics_to_fetch.items():
            scalar_metric_value = self._get_performance_data(host, counter_key)

            if scalar_metric_value is None: # Should not happen if _get_performance_data defaults to 0
                scalar_metric_value = 0.0

            if metric_key == "cpu_usage":      # Counter value is 0-10000
                host_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "memory_usage": # Counter value is 0-10000
                host_metrics[metric_key] = scalar_metric_value / 100.0
            elif metric_key == "disk_io_usage": # Assuming KBps
                host_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            elif metric_key == "network_io_usage": # Assuming KBps
                host_metrics[metric_key] = scalar_metric_value / 1024.0 # Convert to MBps
            else:
                host_metrics[metric_key] = scalar_metric_value

        # Add capacity information
        host_metrics["cpu_capacity"] = host.summary.hardware.numCpuCores * host.summary.hardware.cpuMhz
        host_metrics["memory_capacity"] = host.summary.hardware.memorySize / (1024 * 1024)  # Convert B to MB
        
        # Disk I/O capacity is an estimated value. For consistent balancing based on percentages,
        # this value should be the same for all hosts, or derived using a consistent methodology if dynamic.
        # Current balancing logic (max % - min %) is effective for relative load comparison
        # as long as the capacity figure, even if an estimate, is applied uniformly.
        host_metrics["disk_io_capacity"] = 1000  # Example: 1000 MB/s (estimated)
        
        # Network capacity is derived from the sum of speeds of all physical NICs (pNICs).
        # This provides a more dynamic and host-specific capacity compared to a fixed estimate.
        # If pNIC info is unavailable, a default estimate (10Gbps = 1250.0 MB/s) is used.
        network_capacity_val = 1250.0 # Default value as float
        if (host.config and hasattr(host.config, 'network') and 
            host.config.network and hasattr(host.config.network, 'pnic') and 
            host.config.network.pnic):
            pnics = host.config.network.pnic
            try:
                valid_link_speeds = []
                for pnic_obj in pnics: # Renamed p to pnic_obj for clarity
                    if hasattr(pnic_obj, 'linkSpeed') and \
                       pnic_obj.linkSpeed is not None and \
                       hasattr(pnic_obj.linkSpeed, 'speedMb') and \
                       isinstance(pnic_obj.linkSpeed.speedMb, int): # Check type
                        valid_link_speeds.append(pnic_obj.linkSpeed.speedMb)
                    elif hasattr(pnic_obj, 'linkSpeed') and pnic_obj.linkSpeed is not None and hasattr(pnic_obj.linkSpeed, 'speedMb'):
                        # Log if speedMb exists but is not an int (and not None, covered by isinstance)
                        logger.warning(f"Host '{host.name}', pNIC '{pnic_obj.device}': linkSpeed.speedMb found but is not an integer (type: {type(pnic_obj.linkSpeed.speedMb)} value: {pnic_obj.linkSpeed.speedMb}). Skipping this pNIC for network capacity sum.")

                if valid_link_speeds:
                    total_link_speed_mbps = sum(valid_link_speeds) 
                    network_capacity_val = total_link_speed_mbps / 8.0 
                    if network_capacity_val == 0: # If sum was 0 (e.g. all valid speeds were 0)
                        logger.warning(f"Host '{host.name}': Sum of valid pNIC link speeds is 0. Defaulting network capacity.")
                        network_capacity_val = 1250.0
                else:
                    logger.warning(f"Host '{host.name}': No valid integer link speeds (speedMb) found for pNICs. Defaulting network capacity.")
                    # network_capacity_val remains 1250.0 (default set at start)
            except Exception as e:
                logger.warning(f"Host '{host.name}': Error calculating network capacity from pNICs: {e}. Defaulting.")
                # network_capacity_val remains 1250.0 (default set at start)
        else:
            logger.warning(f"Host '{host.name}': Could not retrieve pNIC information. Defaulting network capacity.")
            # network_capacity_val remains 1250.0 (default set at start)
        host_metrics["network_capacity"] = network_capacity_val

        return host_metrics
