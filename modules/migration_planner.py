import logging
import copy 

logger = logging.getLogger('fdrs')

class MigrationManager:
    def __init__(self, cluster_state, constraint_manager, load_evaluator, aggressiveness=3, max_total_migrations=20, ignore_anti_affinity=False): # Added max_total_migrations and ignore_anti_affinity
        self.cluster_state = cluster_state
        self.constraint_manager = constraint_manager
        self.load_evaluator = load_evaluator
        self.aggressiveness = aggressiveness
        self.ignore_anti_affinity = ignore_anti_affinity
        # Default to 20 if None is explicitly passed, otherwise use the provided value or the parameter default.
        if max_total_migrations is None:
            self.max_total_migrations = 20 # Internal default
        else:
            self.max_total_migrations = int(max_total_migrations) # Ensure it's an int
        # logger setup will use the global logger from fdrs.py, or a module-level logger
        # self.logger = logger # No need for self.logger if using module-level logger

    def _get_simulated_load_data_after_migrations(self, migrations_to_simulate):
        """
        Simulates migrations and returns new CPU/Memory percentage lists and a new map
        reflecting these simulated CPU/Memory loads. Disk/Network are passed through from original state.
        Returns: tuple (sim_cpu_list, sim_mem_list, orig_disk_list, orig_net_list, new_sim_load_map)
        """
        logger.debug(f"[MigrationPlanner_Sim] Simulating {len(migrations_to_simulate)} migrations to update load data.")

        current_absolute_host_loads = {}
        # Use self.cluster_state.hosts as the canonical list of host objects.
        # Ensure that LoadEvaluator also uses this same list or an equivalent ordered list of names.
        # For safety, get the canonical order of host names from LoadEvaluator if possible,
        # or ensure self.cluster_state.hosts is the source of truth for order.
        # The previous version used self.load_evaluator.hosts.
        ordered_host_objects = self.cluster_state.hosts # Assuming this list is stable and representative

        if not ordered_host_objects:
            logger.warning("[MigrationPlanner_Sim] No hosts in cluster_state.hosts. Cannot simulate load changes.")
            return [], [], [], [], {}

        for host_obj in ordered_host_objects:
            if not hasattr(host_obj, 'name'):
                logger.warning(f"[MigrationPlanner_Sim] Host object {host_obj} lacks a name. Skipping for absolute load collection.")
                continue
            host_name = host_obj.name
            host_metrics_from_cs = self.cluster_state.host_metrics.get(host_name, {})

            current_absolute_host_loads[host_name] = {
                'cpu_usage_abs': host_metrics_from_cs.get('cpu_usage', 0), # This should be absolute sum from VMs
                'mem_usage_abs': host_metrics_from_cs.get('memory_usage', 0), # This should be host's overallMemoryUsage
                'cpu_cap_abs': host_metrics_from_cs.get('cpu_capacity', 1), # Avoid division by zero
                'mem_cap_abs': host_metrics_from_cs.get('memory_capacity', 1) # Avoid division by zero
            }

        # Deepcopy to prevent modifying the numbers in self.cluster_state.host_metrics if they are mutable objects (e.g. if not just numbers)
        # For simple numeric values, direct assignment is fine, but deepcopy is safer if structure is complex.
        # Given current structure looks like numbers, direct use for modification is okay.
        # simulated_absolute_loads = copy.deepcopy(current_absolute_host_loads) # If values were complex objects

        # Simulate each migration
        for mig_plan in migrations_to_simulate:
            vm_obj = mig_plan['vm']
            target_host_obj = mig_plan['target_host']
            source_host_obj = self.cluster_state.get_host_of_vm(vm_obj)

            if not hasattr(vm_obj, 'name') or not hasattr(target_host_obj, 'name'):
                logger.warning(f"[MigrationPlanner_Sim] VM or Target Host in migration plan missing name. Skipping: {mig_plan}")
                continue

            vm_name = vm_obj.name
            target_host_name = target_host_obj.name
            source_host_name = source_host_obj.name if source_host_obj and hasattr(source_host_obj, 'name') else None

            vm_res_metrics = self.cluster_state.vm_metrics.get(vm_name, {})
            vm_cpu_abs = vm_res_metrics.get('cpu_usage_abs', 0)
            vm_mem_abs = vm_res_metrics.get('memory_usage_abs', 0)

            if source_host_name and source_host_name in current_absolute_host_loads:
                current_absolute_host_loads[source_host_name]['cpu_usage_abs'] -= vm_cpu_abs
                current_absolute_host_loads[source_host_name]['mem_usage_abs'] -= vm_mem_abs
            elif source_host_name:
                logger.warning(f"[MigrationPlanner_Sim] Source host {source_host_name} for VM {vm_name} not in current_absolute_host_loads. Load not decremented.")

            if target_host_name in current_absolute_host_loads:
                current_absolute_host_loads[target_host_name]['cpu_usage_abs'] += vm_cpu_abs
                current_absolute_host_loads[target_host_name]['mem_usage_abs'] += vm_mem_abs
            else:
                # This was the warning we want to avoid. If current_absolute_host_loads is built from *all* hosts,
                # and target_host_obj is a valid host from the cluster, its name should be a key.
                logger.error(f"[MigrationPlanner_Sim] Target host {target_host_name} for VM {vm_name} not in current_absolute_host_loads. Load not incremented. This indicates an issue with host lists.")

        # Generate new CPU/Memory percentage lists and the simulated map
        sim_cpu_percentages = []
        sim_mem_percentages = []
        sim_host_resource_percentages_map = {}

        # Fetch original Disk/Network I/O percentage lists to pass them through
        # These are fetched once, representing the state before these simulated migrations for these resources.
        _ , _, orig_disk_percentages, orig_net_percentages = self.load_evaluator.get_resource_percentage_lists()

        # Iterate based on the order from LoadEvaluator.hosts to ensure list consistency.
        # This assumes LoadEvaluator.hosts provides a list of host objects/dicts with 'name'.
        # If LoadEvaluator.hosts is just names, we need to adapt.
        # From previous steps, LoadEvaluator takes host data which includes names.

        # Use self.cluster_state.hosts for iteration order, assuming LoadEvaluator uses a compatible order.
        # Best practice would be to use the exact same host list object if possible, or a consistently ordered list of names.
        host_names_from_evaluator = [h.get('name') for h in self.load_evaluator.hosts if isinstance(h, dict) and h.get('name')]
        if not host_names_from_evaluator and ordered_host_objects: # Fallback if load_evaluator.hosts is not structured as list of dicts with names
             host_names_from_evaluator = [h.name for h in ordered_host_objects if hasattr(h, 'name')]


        for i, host_name in enumerate(host_names_from_evaluator):
            sim_loads = current_absolute_host_loads.get(host_name)
            if not sim_loads:
                logger.warning(f"[MigrationPlanner_Sim] Host {host_name} from LoadEvaluator's order not found in simulated loads. Using zeros.")
                cpu_p, mem_p = 0.0, 0.0
            else:
                cpu_p = (sim_loads['cpu_usage_abs'] / sim_loads['cpu_cap_abs'] * 100.0) if sim_loads['cpu_cap_abs'] > 0 else 0
                mem_p = (sim_loads['mem_usage_abs'] / sim_loads['mem_cap_abs'] * 100.0) if sim_loads['mem_cap_abs'] > 0 else 0

            sim_cpu_percentages.append(cpu_p)
            sim_mem_percentages.append(mem_p)

            # Disk and Network percentages are passed through from original
            disk_p = orig_disk_percentages[i] if i < len(orig_disk_percentages) else 0
            net_p = orig_net_percentages[i] if i < len(orig_net_percentages) else 0

            sim_host_resource_percentages_map[host_name] = {
                'cpu': cpu_p, 'memory': mem_p,
                'disk': disk_p, 'network': net_p
            }

        logger.debug(f"[MigrationPlanner_Sim] Simulation complete. New load map: {sim_host_resource_percentages_map}")
        return sim_cpu_percentages, sim_mem_percentages, orig_disk_percentages, orig_net_percentages, sim_host_resource_percentages_map

    def _is_anti_affinity_safe(self, vm_to_move, target_host_obj, planned_migrations_in_cycle=None):
        logger.debug(f"[MigrationPlanner] Checking anti-affinity safety for VM '{vm_to_move.name}' to host '{target_host_obj.name}'. Planned migrations in cycle: {planned_migrations_in_cycle}")
        vm_prefix = vm_to_move.name[:-2]
        
        # Ensure vm_distribution is populated. It should be after constraint_manager.apply()
        if not self.constraint_manager.vm_distribution:
            logger.warning("[MigrationPlanner_AA_Check] vm_distribution is empty. Forcing population.")
            self.constraint_manager.enforce_anti_affinity() # Should ideally be populated before planning

        vms_in_group = self.constraint_manager.vm_distribution.get(vm_prefix, [])
        if not vms_in_group:
            logger.debug(f"[MigrationPlanner_AA_Check] VM '{vm_to_move.name}' (prefix '{vm_prefix}') not in any anti-affinity group. Safe.")
            return True

        source_host_obj = self.cluster_state.get_host_of_vm(vm_to_move)
        # source_host_name will be None if vm_to_move is not currently on a host (e.g. new VM)
        # or if it's already been hypothetically removed in a simulation.
        # For balancing, it should always have a source_host_obj.
        source_host_name = source_host_obj.name if source_host_obj else None

        all_active_hosts = self.cluster_state.hosts # Use direct attribute
        if not all_active_hosts or len(all_active_hosts) <= 1:
            logger.debug("[MigrationPlanner_AA_Check] Not enough active hosts (<2) for anti-affinity to apply. Safe.")
            return True

        simulated_host_vm_counts = {host.name: 0 for host in all_active_hosts if hasattr(host, 'name')}

        planned_vm_locations = {}
        if planned_migrations_in_cycle:
            for plan in planned_migrations_in_cycle:
                if hasattr(plan['vm'], 'name') and hasattr(plan['target_host'], 'name'):
                    planned_vm_locations[plan['vm'].name] = plan['target_host'].name

        for vm_in_group_iter in vms_in_group:
            if not hasattr(vm_in_group_iter, 'name'):
                logger.warning(f"[MigrationPlanner_AA_Check] VM in group {vm_prefix} is missing a name. Skipping.")
                continue

            current_vm_name = vm_in_group_iter.name
            final_host_name_for_iter_vm = None

            if current_vm_name == vm_to_move.name:
                final_host_name_for_iter_vm = target_host_obj.name
            elif current_vm_name in planned_vm_locations:
                final_host_name_for_iter_vm = planned_vm_locations[current_vm_name]
            else:
                host_obj = self.cluster_state.get_host_of_vm(vm_in_group_iter)
                if host_obj and hasattr(host_obj, 'name'):
                    final_host_name_for_iter_vm = host_obj.name

            if final_host_name_for_iter_vm and final_host_name_for_iter_vm in simulated_host_vm_counts:
                simulated_host_vm_counts[final_host_name_for_iter_vm] += 1
        
        counts = [count for host_name, count in simulated_host_vm_counts.items() if self.cluster_state.get_host_by_name(host_name)] # Only count active hosts
        
        if not counts:
            logger.debug(f"[MigrationPlanner_AA_Check] No VMs from group '{vm_prefix}' found on any active host in simulation. Safe.")
            return True
        
        is_safe = max(counts) - min(counts) <= 1
        if not is_safe:
            logger.warning(f"[MigrationPlanner_AA_Check] VM '{vm_to_move.name}' to host '{target_host_obj.name}' is NOT anti-affinity safe. Counts: {simulated_host_vm_counts}, MaxDiff: {max(counts) - min(counts)}")
        else:
            logger.debug(f"[MigrationPlanner_AA_Check] VM '{vm_to_move.name}' to host '{target_host_obj.name}' IS anti-affinity safe. Counts: {simulated_host_vm_counts}")
        return is_safe

    def _would_fit_on_host(self, vm, host_obj):
        logger.debug(f"[MigrationPlanner] Checking if VM '{vm.name}' would fit on host '{host_obj.name}'.")
        # Use high watermarks to prevent total host overload, not for balancing.
        # These are absolute limits for a single host.
        # Example: Don't allow a move if host CPU would exceed 90% or MEM 90%.
        # These thresholds are distinct from LoadEvaluator's balancing thresholds.
        # This method needs access to VM's resource requirements and host's current load + capacity.

        vm_metrics = self.cluster_state.vm_metrics.get(vm.name, {})
        host_current_metrics = self.cluster_state.host_metrics.get(host_obj.name, {})
        # host_capacity is part of host_current_metrics

        if not vm_metrics or not host_current_metrics: # host_capacity removed from this check
            logger.warning(f"[MigrationPlanner_FitCheck] Missing metrics for VM '{vm.name}' or host '{host_obj.name}'. Cannot perform fit check.")
            return False

        # Define absolute maximums (can be configurable)
        # These are % of TOTAL capacity
        max_cpu_util_post_move = 90.0 
        max_mem_util_post_move = 90.0
        # Add other resources like disk, network if relevant for "fit"
        
        # VM requirements (ensure these keys exist in your vm_metrics)
        vm_cpu_req = vm_metrics.get('cpu_usage_abs', vm_metrics.get('cpu_allocation', 0)) # Absolute CPU units (e.g., MHz)
        vm_mem_req = vm_metrics.get('memory_usage_abs', vm_metrics.get('memory_allocation_bytes', 0)) # Absolute Memory (e.g., Bytes)

        # Host capacity (ensure these keys exist in host_current_metrics)
        host_cpu_cap = host_current_metrics.get('cpu_capacity', 1) # Total CPU (from host_metrics)
        host_mem_cap = host_current_metrics.get('memory_capacity', 1) # Total Memory (from host_metrics)

        # Host current usage (ensure these keys exist in your host_current_metrics)
        # Note: 'cpu_usage_abs' for hosts is not directly stored; 'cpu_usage' in host_metrics is sum of VM abs.
        # This logic assumes host_current_metrics contains the summed absolute usage.
        host_cpu_curr = host_current_metrics.get('cpu_usage', 0) # Sum of VM absolute CPU usage from host_metrics
        host_mem_curr = host_current_metrics.get('memory_usage_abs', 0) # Current absolute memory usage

        projected_cpu_abs = host_cpu_curr + vm_cpu_req
        projected_mem_abs = host_mem_curr + vm_mem_req

        projected_cpu_pct = (projected_cpu_abs / host_cpu_cap * 100.0) if host_cpu_cap > 0 else 100.0
        projected_mem_pct = (projected_mem_abs / host_mem_cap * 100.0) if host_mem_cap > 0 else 100.0

        if projected_cpu_pct > max_cpu_util_post_move:
            logger.info(f"[MigrationPlanner_FitCheck] VM '{vm.name}' would not fit on host '{host_obj.name}' due to CPU (proj: {projected_cpu_pct:.1f}% > max: {max_cpu_util_post_move:.1f}%)")
            return False
        if projected_mem_pct > max_mem_util_post_move:
            logger.info(f"[MigrationPlanner_FitCheck] VM '{vm.name}' would not fit on host '{host_obj.name}' due to Memory (proj: {projected_mem_pct:.1f}% > max: {max_mem_util_post_move:.1f}%)")
            return False
        
        logger.debug(f"[MigrationPlanner_FitCheck] VM '{vm.name}' would fit on host '{host_obj.name}'. Proj CPU: {projected_cpu_pct:.1f}%, Proj Mem: {projected_mem_pct:.1f}%")
        return True

    def _select_vms_to_move(self, source_host_obj, imbalanced_resource=None, vms_already_in_plan=None):
        logger.debug(f"[MigrationPlanner] Selecting VMs to move from host '{source_host_obj.name}'. Imbalanced: {imbalanced_resource}")
        if vms_already_in_plan is None: vms_already_in_plan = set()

        vms_on_host = self.cluster_state.get_vms_on_host(source_host_obj)
        if not vms_on_host:
            return []

        candidate_vms = []
        for vm in vms_on_host:
            if vm.name in vms_already_in_plan: # VM object or VM name
                logger.debug(f"[MigrationPlanner_SelectVMs] VM '{vm.name}' already in migration plan. Skipping.")
                continue
            if hasattr(vm, 'config') and getattr(vm.config, 'template', False):
                logger.debug(f"[MigrationPlanner_SelectVMs] Skipping template VM '{vm.name}' for selection.")
                continue
            candidate_vms.append(vm)
        
        # Sort VMs by their contribution to the imbalanced resource, or general load if no specific resource
        # This requires VM metrics (cpu_usage_abs, memory_usage_abs etc.)
        def sort_key(vm):
            metrics = self.cluster_state.vm_metrics.get(vm.name, {})
            if not metrics: return 0
            if imbalanced_resource == 'cpu':
                return metrics.get('cpu_usage_abs', 0) # Absolute CPU usage
            elif imbalanced_resource == 'memory':
                return metrics.get('memory_usage_abs', 0) # Absolute Memory usage
            # Add disk/network if they are part of imbalance evaluation
            else: # General load: sum of normalized % usages or absolute values if comparable
                  # Using absolute values for simplicity if available and somewhat comparable
                return metrics.get('cpu_usage_abs', 0) + metrics.get('memory_usage_abs', 0)

        candidate_vms.sort(key=sort_key, reverse=True)
        
        # Select a limited number of VMs based on aggressiveness (e.g., 1 to 3)
        # This is a simplification; a more complex selection might consider VM sizes relative to imbalance
        num_to_select = self.aggressiveness 
        selected = candidate_vms[:num_to_select]
        logger.info(f"[MigrationPlanner_SelectVMs] Selected {len(selected)} VMs from '{source_host_obj.name}': {[vm.name for vm in selected]}")
        return selected

    def _find_better_host_for_balancing(self, vm_to_move, source_host_obj, all_hosts, imbalanced_resources_details, host_resource_percentages_map, planned_migrations_in_cycle=None):
        """
        Finds a more suitable host for a VM to improve resource balance.
        Considers host capacity, anti-affinity rules (with planned migrations), and target host load.
        Uses host_resource_percentages_map for target host metrics.
        planned_migrations_in_cycle is a list of dicts of already planned moves in this cycle.
        """
        logger.debug(f"[MigrationPlanner] Finding better host for VM '{vm_to_move.name}' from '{source_host_obj.name}' for balancing.")
        potential_targets = []

        for target_host_obj in all_hosts:
            if not hasattr(target_host_obj, 'name') or target_host_obj.name == source_host_obj.name:
                continue

            if not self._would_fit_on_host(vm_to_move, target_host_obj):
                continue

            # Pass planned_migrations_in_cycle to the anti-affinity check
            if not self.ignore_anti_affinity:
                if not self._is_anti_affinity_safe(vm_to_move, target_host_obj, planned_migrations_in_cycle=planned_migrations_in_cycle):
                    logger.debug(f"[MigrationPlanner_FindBetterHost] Host '{target_host_obj.name}' skipped for VM '{vm_to_move.name}' due to anti-affinity rules (ignore_anti_affinity is False).")
                    continue
            else:
                logger.debug(f"[MigrationPlanner_FindBetterHost] Anti-affinity check bypassed for VM '{vm_to_move.name}' to host '{target_host_obj.name}' (ignore_anti_affinity is True).")

            score = 0
            # Get target host's metrics from the provided map
            target_metrics_pct = host_resource_percentages_map.get(target_host_obj.name)
            
            if not target_metrics_pct:
                 logger.warning(f"[MigrationPlanner_FindBetterHost] Could not get metrics for target host '{target_host_obj.name}' from provided map. Skipping.")
                 continue

            # Score based on how much it improves balance for imbalanced resources
            # Lower utilization on target host for imbalanced resources is better.
            for resource, detail in imbalanced_resources_details.items():
                 if resource in target_metrics_pct:
                     # Higher score if target is less utilized for this imbalanced resource
                     score += (100 - target_metrics_pct[resource]) 
            
            if score > 0:
                potential_targets.append({'host': target_host_obj, 'score': score})
        
        if not potential_targets:
            logger.info(f"[MigrationPlanner_FindBetterHost] No suitable balancing target host found for VM '{vm_to_move.name}'.")
            return None

        # Sort potential targets by score (higher is better)
        potential_targets.sort(key=lambda x: x['score'], reverse=True)
        best_target = potential_targets[0]['host']
        logger.info(f"[MigrationPlanner_FindBetterHost] Best balancing target for VM '{vm_to_move.name}' is '{best_target.name}' with score {potential_targets[0]['score']}.")
        return best_target

    def plan_migrations(self):
        logger.info("[MigrationPlanner] Starting migration planning cycle...")
        migrations = []
        # Use a set to keep track of VMs already planned to move, to avoid duplicate moves.
        # Store VM names for simplicity, assuming names are unique identifiers.
        vms_in_migration_plan = set()

        # Get initial host resource percentages map and lists from LoadEvaluator
        initial_host_resource_percentages_map = {}
        if hasattr(self.load_evaluator, 'get_all_host_resource_percentages_map'):
            initial_host_resource_percentages_map = self.load_evaluator.get_all_host_resource_percentages_map()
            logger.debug(f"[MigrationPlanner] Fetched initial host_resource_percentages_map from LoadEvaluator.")
        else:
            logger.error("[MigrationPlanner] Critical: self.load_evaluator.get_all_host_resource_percentages_map() not found. Balancing will be severely impaired.")
            initial_host_resource_percentages_map = {}

        # These are needed for fallback if simulation for disk/net is not possible
        # and also as a base for _get_simulated_load_lists_after_migrations if it needs them.
        # This call might be redundant if _get_simulated_load_lists_after_migrations fetches them itself, which it does now.
        # initial_cpu_p_list, initial_mem_p_list, initial_disk_p_list, initial_net_p_list = self.load_evaluator.get_resource_percentage_lists()

        # Step 1: Addressing Anti-Affinity violations
        anti_affinity_migrations = self._plan_anti_affinity_migrations(vms_in_migration_plan)
        migrations.extend(anti_affinity_migrations)
        logger.info(f"[MigrationPlanner] After Anti-Affinity, {len(anti_affinity_migrations)} migrations planned.")

        # Current load map and lists reflect the state *after* AA migrations for balancing decisions
        current_host_resource_percentages_map = initial_host_resource_percentages_map
        # Initialize override lists. If no AA migrations, these will be None, and LoadEvaluator will use its internal lists.
        sim_cpu_p_override, sim_mem_p_override, sim_disk_p_override, sim_net_p_override = None, None, None, None

        if anti_affinity_migrations:
            logger.info("[MigrationPlanner] Simulating anti-affinity migrations to re-evaluate load balance...")
            sim_cpu_p_override, sim_mem_p_override, sim_disk_p_override, sim_net_p_override, simulated_load_map_after_aa = \
                self._get_simulated_load_data_after_migrations(anti_affinity_migrations) # Removed initial_host_load_map from call
            current_host_resource_percentages_map = simulated_load_map_after_aa
            logger.info("[MigrationPlanner] Load balance re-evaluation will use simulated state after AA migrations.")
        else:
            logger.info("[MigrationPlanner] No anti-affinity migrations, proceeding with initial load state for balancing.")

        # Step 2: Addressing Resource Imbalance, using potentially simulated state
        # Pass the override lists to evaluate_imbalance via _plan_balancing_migrations
        balancing_migrations = self._plan_balancing_migrations(
            vms_in_migration_plan,
            current_host_resource_percentages_map, # This is the (potentially simulated) map
            migrations, # Pass all migrations so far (AA + any prior balancing if iterative in future)
            sim_cpu_p_override, # Pass simulated lists for evaluate_imbalance
            sim_mem_p_override,
            sim_disk_p_override,
            sim_net_p_override
        )
        migrations.extend(balancing_migrations)

        logger.info(f"[MigrationPlanner] After Resource Balancing, {len(migrations)} total migrations planned.")

        # Enforce overall migration limit (Part 2)
        if len(migrations) > self.max_total_migrations:
            logger.warning(f"[MigrationPlanner] Planned migrations ({len(migrations)}) exceed max limit ({self.max_total_migrations}). Truncating.")
            # Prioritize Anti-Affinity migrations
            final_limited_migrations = []
            aa_migs_from_plan = [m for m in migrations if m.get('reason') == 'Anti-Affinity']
            balance_migs_from_plan = [m for m in migrations if m.get('reason') != 'Anti-Affinity'] # Crude, refine if more reasons

            if len(aa_migs_from_plan) >= self.max_total_migrations:
                final_limited_migrations = aa_migs_from_plan[:self.max_total_migrations]
                logger.info(f"[MigrationPlanner] Truncated to only {len(final_limited_migrations)} anti-affinity migrations.")
            else:
                final_limited_migrations.extend(aa_migs_from_plan)
                remaining_slots = self.max_total_migrations - len(final_limited_migrations)
                if remaining_slots > 0 and balance_migs_from_plan:
                    final_limited_migrations.extend(balance_migs_from_plan[:remaining_slots])
                    logger.info(f"[MigrationPlanner] Took all {len(aa_migs_from_plan)} AA migrations and {len(balance_migs_from_plan[:remaining_slots])} balancing migrations.")
                else:
                     logger.info(f"[MigrationPlanner] Only anti-affinity migrations included after limit. Count: {len(final_limited_migrations)}")

            migrations = final_limited_migrations
            logger.info(f"[MigrationPlanner] Final migration count after truncation: {len(migrations)}")


        if not migrations:
            logger.info("[MigrationPlanner] No migrations planned in this cycle.")
        else:
            logger.info(f"[MigrationPlanner] Total final migrations planned: {len(migrations)}")
            for i, mig_plan in enumerate(migrations):
                logger.info(f"  {i+1}. VM: {mig_plan['vm'].name}, Target: {mig_plan['target_host'].name}, Reason: {mig_plan['reason']}")

        final_migration_tuples = [(plan['vm'], plan['target_host']) for plan in migrations]
        return final_migration_tuples


    def _plan_anti_affinity_migrations(self, vms_in_migration_plan):
        """
        Plans migrations to address anti-affinity violations.
        Updates vms_in_migration_plan with VMs planned for migration.
        Returns a list of migration dictionaries.
        """
        logger.info("[MigrationPlanner] Step 1: Addressing Anti-Affinity violations.")
        all_aa_migrations_for_return = [] # List to be returned by this method
        aa_migrations_planned_this_step = [] # Local list for this AA planning pass

        # Ensure violations are calculated if not already present
        if not hasattr(self.constraint_manager, 'violations') or not self.constraint_manager.violations:
            logger.info("[MigrationPlanner_AA] No pre-calculated AA violations found or list is empty. Attempting to calculate now.")
            self.constraint_manager.enforce_anti_affinity() # Ensure groups are up-to-date
            self.constraint_manager.violations = self.constraint_manager.calculate_anti_affinity_violations() # Store them

        anti_affinity_vm_violations = self.constraint_manager.violations
        if not anti_affinity_vm_violations:
            logger.info("[MigrationPlanner_AA] No anti-affinity violations found after calculation.")
            return []

        logger.debug(f"[MigrationPlanner_AA] Processing {len(anti_affinity_vm_violations)} potential anti-affinity violating VMs.")

        for vm_obj in anti_affinity_vm_violations:
            if not hasattr(vm_obj, 'name'):
                logger.warning("[MigrationPlanner_AA] Found VM in AA violations list without a name. Skipping.")
                continue

            if vm_obj.name in vms_in_migration_plan:
                logger.debug(f"[MigrationPlanner_AA] VM '{vm_obj.name}' already part of another migration plan. Skipping for AA.")
                continue
            
            if hasattr(vm_obj, 'config') and getattr(vm_obj.config, 'template', False):
                logger.debug(f"[MigrationPlanner_AA] Skipping template VM '{vm_obj.name}' for anti-affinity migration.")
                continue

            current_host = self.cluster_state.get_host_of_vm(vm_obj)
            logger.info(f"[MigrationPlanner_AA] VM '{vm_obj.name}' violates anti-affinity on host '{current_host.name if current_host else 'Unknown'}'. Finding preferred host.")
            
            # Pass the migrations planned so far *in this AA step*
            target_host_obj = self.constraint_manager.get_preferred_host_for_vm(
                vm_obj,
                planned_migrations_this_cycle=aa_migrations_planned_this_step
            )

            if target_host_obj:
                if self._would_fit_on_host(vm_obj, target_host_obj):
                    migration_plan = {'vm': vm_obj, 'target_host': target_host_obj, 'reason': 'Anti-Affinity'}
                    all_aa_migrations_for_return.append(migration_plan)
                    aa_migrations_planned_this_step.append(migration_plan) # Add to list for next iteration's consideration
                    vms_in_migration_plan.add(vm_obj.name) # Add to global set passed in
                    logger.info(f"[MigrationPlanner_AA] Planned Anti-Affinity Migration: Move VM '{vm_obj.name}' from '{current_host.name if current_host else 'N/A'}' to '{target_host_obj.name}'.")
                else:
                    logger.warning(f"[MigrationPlanner_AA] Preferred host '{target_host_obj.name}' for VM '{vm_obj.name}' cannot fit it. No AA migration planned for this VM.")
            else:
                logger.warning(f"[MigrationPlanner_AA] No suitable preferred host found for anti-affinity violating VM '{vm_obj.name}'.")
        return all_aa_migrations_for_return

    def _plan_balancing_migrations(self, vms_in_migration_plan,
                                 host_resource_percentages_map_for_decision,
                                 current_planned_migrations_list,
                                 sim_cpu_p_override, sim_mem_p_override,
                                 sim_disk_p_override, sim_net_p_override):
        """
        Plans migrations to address resource imbalances.
        Uses host_resource_percentages_map_for_decision for selecting source/target hosts.
        Uses sim_*_override lists when calling evaluate_imbalance.
        current_planned_migrations_list includes AA moves for _is_anti_affinity_safe checks.
        """
        logger.info("[MigrationPlanner] Step 2: Addressing Resource Imbalance.")
        balancing_migrations = []

        # Evaluate imbalance using potentially simulated percentage lists
        imbalance_details = self.load_evaluator.evaluate_imbalance(
            aggressiveness=self.aggressiveness,
            cpu_percentages_override=sim_cpu_p_override,
            mem_percentages_override=sim_mem_p_override,
            disk_percentages_override=sim_disk_p_override,
            net_percentages_override=sim_net_p_override
        )

        if not imbalance_details:
            logger.info("[MigrationPlanner_Balance] Cluster is already balanced (possibly after simulation) or no imbalance details found.")
            return []

        logger.info("[MigrationPlanner_Balance] Cluster imbalance details (post-AA sim if any):")
        if imbalance_details:
            for resource_name, details in imbalance_details.items():
                details_str = f"  Resource: {resource_name}"
                details_str += f", Imbalanced: {details.get('is_imbalanced')}"
                details_str += f", Diff: {details.get('current_diff', 0):.2f}%"
                details_str += f", Threshold: {details.get('threshold', 0):.2f}%"
                details_str += f", Min: {details.get('min_usage', 0):.2f}%"
                details_str += f", Max: {details.get('max_usage', 0):.2f}%"
                details_str += f", Avg: {details.get('avg_usage', 0):.2f}%"
                logger.info(details_str)
        else:
            logger.info("  No imbalance details found or cluster is balanced.") # Should not be reached if prior check handles empty imbalance_details

        all_hosts_objects = self.cluster_state.hosts # These are host objects/dicts from cluster_state

        problematic_resources_names = [res for res, det in imbalance_details.items() if det.get('is_imbalanced')]
        if not problematic_resources_names:
            logger.info("[MigrationPlanner_Balance] No specific resource marked as imbalanced after (potential) simulation. Skipping balancing moves.")
            return []
        
        logger.info(f"[MigrationPlanner_Balance] Problematic resources for balancing (post-AA sim): {problematic_resources_names}")

        # current_planned_migrations_list already contains AA migrations.
        # We will append balancing migrations to it as they are decided for subsequent AA checks.
        # Make a copy to modify locally within this balancing phase for iterative safety checks.
        safety_check_migrations_list = current_planned_migrations_list[:]


        for source_host_obj in all_hosts_objects:
            if not hasattr(source_host_obj, 'name'): continue
            # Use the host_resource_percentages_map_for_decision for picking source hosts
            source_host_metrics_pct = host_resource_percentages_map_for_decision.get(source_host_obj.name, {})

            move_reason_details = []
            # Phase 3 change: Use max_usage from imbalance_details to identify source hosts
            host_is_max_usage_contributor = False
            resource_hint_for_vm_selection = None # Will be based on why this host is a source

            for res_name in problematic_resources_names:
                res_detail = imbalance_details.get(res_name, {})
                # res_threshold = self.load_evaluator.get_thresholds(self.aggressiveness).get(res_name) # Not used directly for source host selection anymore
                
                # Check if this host's usage for 'res_name' matches the max_usage for that resource
                current_host_usage_for_res = source_host_metrics_pct.get(res_name, 0)
                max_usage_for_res = res_detail.get('max_usage', -1) # max_usage for the resource across cluster

                if current_host_usage_for_res == max_usage_for_res and current_host_usage_for_res > 0 : # Host is one of the max usage hosts
                    # Ensure that this max_usage is actually part of an imbalance (i.e. max_usage > min_usage + threshold)
                    # This is implicitly covered by res_detail.get('is_imbalanced')
                    host_is_max_usage_contributor = True
                    reason_str = f"max_usage_host for {res_name} ({current_host_usage_for_res:.1f}%)"
                    move_reason_details.append(reason_str)
                    if not resource_hint_for_vm_selection: # Prioritize first identified imbalanced resource
                        resource_hint_for_vm_selection = res_name


            if not host_is_max_usage_contributor:
                continue

            logger.debug(f"[MigrationPlanner_Balance] Host '{source_host_obj.name}' is a candidate source. Reasons: {', '.join(move_reason_details)}")

            # Select VMs to move from this source host
            # Pass vms_in_migration_plan (overall set) to _select_vms_to_move to avoid re-planning a VM
            candidate_vms_to_move = self._select_vms_to_move(source_host_obj, resource_hint_for_vm_selection, vms_in_migration_plan)

            for vm_to_move in candidate_vms_to_move:
                # _select_vms_to_move ensures vm_to_move.name is not in vms_in_migration_plan.
                # If selected, it's a valid candidate for now.

                active_imbalance_details_for_target_finding = {
                     k: v for k,v in imbalance_details.items() if k in problematic_resources_names and v.get('is_imbalanced')
                }
                if not active_imbalance_details_for_target_finding:
                     logger.debug(f"No active imbalance details to guide target host finding for VM {vm_to_move.name}. Skipping.")
                     continue

                # Use host_resource_percentages_map_for_decision for finding better host,
                # and safety_check_migrations_list for _is_anti_affinity_safe
                target_host_obj = self._find_better_host_for_balancing(
                    vm_to_move,
                    source_host_obj,
                    all_hosts_objects, # List of actual host objects/dicts
                    active_imbalance_details_for_target_finding,
                    host_resource_percentages_map_for_decision, # The (potentially) simulated map
                    planned_migrations_in_cycle=safety_check_migrations_list
                )

                if target_host_obj:
                    migration_details = {'vm': vm_to_move, 'target_host': target_host_obj, 'reason': f"Resource Balancing ({', '.join(move_reason_details)})"}
                    balancing_migrations.append(migration_details)
                    vms_in_migration_plan.add(vm_to_move.name) # Add to overall set to prevent re-planning
                    safety_check_migrations_list.append(migration_details) # Add to local list for subsequent safety checks in this phase
                    logger.info(f"[MigrationPlanner_Balance] Planned Balancing Migration: Move VM '{vm_to_move.name}' from '{source_host_obj.name}' to '{target_host_obj.name}'.")
                else:
                    logger.info(f"[MigrationPlanner_Balance] No suitable balancing target found for VM '{vm_to_move.name}' from host '{source_host_obj.name}'.")

        return balancing_migrations

    def execute_migrations(self, migration_tuples):
        """Execute the planned migrations (list of (VM object, TargetHost object) tuples)"""
        if not migration_tuples:
            logger.info("[MigrationExecutor] No migrations to execute.")
            return

        logger.info(f"[MigrationExecutor] Executing {len(migration_tuples)} migrations...")
        for vm_obj, target_host_obj in migration_tuples:
            source_host_obj = self.cluster_state.get_host_of_vm(vm_obj)
            source_host_name = source_host_obj.name if source_host_obj else "Unknown (already moved or new?)"
            
            try:
                logger.info(f"Attempting migration of VM '{vm_obj.name}' from '{source_host_name}' to '{target_host_obj.name}'...")             
                logger.success(f"SUCCESS: Migration of '{vm_obj.name}' from '{source_host_name}' to '{target_host_obj.name}' completed (simulated).")
            except Exception as e:
                logger.error(f"FAILED: Migration of '{vm_obj.name}' from '{source_host_name}' to '{target_host_obj.name}' failed: {str(e)}")
