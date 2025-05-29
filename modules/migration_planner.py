import logging
# Assuming Logger is now standard logging
logger = logging.getLogger('fdrs')

class MigrationManager:
    def __init__(self, cluster_state, constraint_manager, load_evaluator, aggressiveness=3):
        self.cluster_state = cluster_state
        self.constraint_manager = constraint_manager
        self.load_evaluator = load_evaluator
        self.aggressiveness = aggressiveness
        # logger setup will use the global logger from fdrs.py, or a module-level logger
        # self.logger = logger # No need for self.logger if using module-level logger

    def _is_anti_affinity_safe(self, vm_to_move, target_host_obj):
        logger.debug(f"[MigrationPlanner] Checking anti-affinity safety for VM '{vm_to_move.name}' to host '{target_host_obj.name}'.")
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

        all_active_hosts = self.cluster_state.get_all_hosts()
        if not all_active_hosts or len(all_active_hosts) <= 1:
            logger.debug("[MigrationPlanner_AA_Check] Not enough active hosts (<2) for anti-affinity to apply. Safe.")
            return True

        simulated_host_vm_counts = {host.name: 0 for host in all_active_hosts if hasattr(host, 'name')}

        for vm_in_group_iter in vms_in_group:
            if not hasattr(vm_in_group_iter, 'name'): 
                logger.warning(f"[MigrationPlanner_AA_Check] VM in group {vm_prefix} is missing a name. Skipping.")
                continue

            current_host_of_iter_vm = self.cluster_state.get_host_of_vm(vm_in_group_iter)
            
            if vm_in_group_iter.name == vm_to_move.name:
                # This VM is hypothetically moved to target_host_obj
                if target_host_obj.name in simulated_host_vm_counts:
                    simulated_host_vm_counts[target_host_obj.name] += 1
                # If target_host_obj.name is not in simulated_host_vm_counts, it's an inactive/invalid host
                # This case should ideally be filtered out before calling _is_anti_affinity_safe
            else:
                if current_host_of_iter_vm and hasattr(current_host_of_iter_vm, 'name') and current_host_of_iter_vm.name in simulated_host_vm_counts:
                    simulated_host_vm_counts[current_host_of_iter_vm.name] += 1
        
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

        vm_metrics = self.cluster_state.get_vm_metrics(vm.name) # Needs to provide actual usage or allocation
        host_current_metrics = self.cluster_state.get_host_metrics(host_obj.name) # Current usage
        host_capacity = self.cluster_state.get_host_capacity(host_obj.name) # Total capacity

        if not vm_metrics or not host_current_metrics or not host_capacity:
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

        # Host capacity (ensure these keys exist in your host_capacity)
        host_cpu_cap = host_capacity.get('cpu_capacity_mhz', 1) # Total CPU
        host_mem_cap = host_capacity.get('memory_capacity_bytes', 1) # Total Memory

        # Host current usage (ensure these keys exist in your host_current_metrics)
        host_cpu_curr = host_current_metrics.get('cpu_usage_abs', 0) # Current absolute CPU usage
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
            metrics = self.cluster_state.get_vm_metrics(vm.name)
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

    def _find_better_host_for_balancing(self, vm_to_move, source_host_obj, all_hosts, imbalanced_resources_details):
        logger.debug(f"[MigrationPlanner] Finding better host for VM '{vm_to_move.name}' from '{source_host_obj.name}' for balancing.")
        potential_targets = []

        for target_host_obj in all_hosts:
            if not hasattr(target_host_obj, 'name') or target_host_obj.name == source_host_obj.name:
                continue

            if not self._would_fit_on_host(vm_to_move, target_host_obj):
                continue

            if not self._is_anti_affinity_safe(vm_to_move, target_host_obj):
                continue

            # Score based on how much it improves balance for imbalanced resources
            # Lower utilization on target host for imbalanced resources is better.
            # This is a simplified scoring. A more advanced one would check the post-move balance.
            score = 0
            target_metrics_pct = self.load_evaluator.get_resource_percentage_lists_for_host(target_host_obj.name) # Needs new method in LoadEvaluator
            
            if not target_metrics_pct: # If host has no metrics, skip
                 logger.warning(f"Could not get metrics for target {target_host_obj.name}, skipping.")
                 continue


            for resource, detail in imbalanced_resources_details.items(): # e.g. {'cpu': {'current_diff': 25, 'threshold': 15}}
                 if resource in target_metrics_pct:
                     # Higher score if target is less utilized for this imbalanced resource
                     score += (100 - target_metrics_pct[resource]) 
            
            if score > 0:
                potential_targets.append({'host': target_host_obj, 'score': score})
        
        if not potential_targets:
            logger.info(f"[MigrationPlanner_FindBetterHost] No suitable balancing target host found for VM '{vm_to_move.name}'.")
            return None

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

        # 1. Anti-Affinity Violations
        # constraint_manager.violations should be populated by constraint_manager.apply() before this
        logger.info("[MigrationPlanner] Step 1: Addressing Anti-Affinity violations.")
        if not hasattr(self.constraint_manager, 'violations'):
            logger.error("[MigrationPlanner] ConstraintManager has no 'violations' attribute. Run apply() first.")
            anti_affinity_vm_violations = []
        else:
            anti_affinity_vm_violations = self.constraint_manager.violations # List of VM objects

        if not anti_affinity_vm_violations and hasattr(self.constraint_manager, 'calculate_anti_affinity_violations'):
             logger.info("[MigrationPlanner] No pre-calculated AA violations, trying to calculate now.")
             # This is a fallback, ideally apply() is called before plan_migrations()
             self.constraint_manager.enforce_anti_affinity() # Ensure groups are up-to-date
             anti_affinity_vm_violations = self.constraint_manager.calculate_anti_affinity_violations()


        for vm_obj in anti_affinity_vm_violations:
            if not hasattr(vm_obj, 'name'):
                logger.warning("[MigrationPlanner_AA] Found VM in AA violations list without a name. Skipping.")
                continue

            if vm_obj.name in vms_in_migration_plan:
                logger.info(f"[MigrationPlanner_AA] VM '{vm_obj.name}' already part of another migration plan. Skipping for AA.")
                continue
            
            if hasattr(vm_obj, 'config') and getattr(vm_obj.config, 'template', False):
                logger.info(f"[MigrationPlanner_AA] Skipping template VM '{vm_obj.name}' for anti-affinity migration.")
                continue

            current_host = self.cluster_state.get_host_of_vm(vm_obj)
            logger.info(f"[MigrationPlanner_AA] VM '{vm_obj.name}' violates anti-affinity on host '{current_host.name if current_host else 'Unknown'}'. Finding preferred host.")
            
            target_host_obj = self.constraint_manager.get_preferred_host_for_vm(vm_obj) # This method should return a host object

            if target_host_obj:
                if self._would_fit_on_host(vm_obj, target_host_obj): # Ensure target can actually take it
                    migrations.append({'vm': vm_obj, 'target_host': target_host_obj, 'reason': 'Anti-Affinity'})
                    vms_in_migration_plan.add(vm_obj.name)
                    logger.info(f"[MigrationPlanner_AA] Planned Anti-Affinity Migration: Move VM '{vm_obj.name}' from '{current_host.name if current_host else 'N/A'}' to '{target_host_obj.name}'.")
                else:
                    logger.warning(f"[MigrationPlanner_AA] Preferred host '{target_host_obj.name}' for VM '{vm_obj.name}' cannot fit it. No AA migration planned for this VM.")
            else:
                logger.warning(f"[MigrationPlanner_AA] No suitable preferred host found for anti-affinity violating VM '{vm_obj.name}'.")
        
        logger.info(f"[MigrationPlanner] After Anti-Affinity, {len(migrations)} migrations planned.")

        # 2. Resource Balancing
        logger.info("[MigrationPlanner] Step 2: Addressing Resource Imbalance.")
        # evaluate_imbalance now returns a dict like {'cpu': {'is_imbalanced': True, 'max_diff': 25}, ...} or None if balanced
        imbalance_details = self.load_evaluator.evaluate_imbalance(aggressiveness=self.aggressiveness) 

        if imbalance_details: # If dict is not empty (i.e. some imbalance exists)
            logger.info(f"[MigrationPlanner_Balance] Cluster is imbalanced. Details: {imbalance_details}")
            
            # Identify most and least loaded hosts based on overall or specific imbalances
            # This needs LoadEvaluator to provide sorted host lists or detailed metrics per host
            all_hosts_objects = self.cluster_state.get_all_hosts()
            
            # Create a list of hosts sorted by overall utilization (descending) - simplistic approach
            # A more robust approach would be to get this from LoadEvaluator or sort based on the specific imbalanced metrics
            # For now, just get all hosts and iterate. _find_better_host_for_balancing will pick less loaded ones.
            # We need to define source_hosts based on which resources are imbalanced.
            
            # Get host utilization percentages for sorting
            # This is a placeholder for getting detailed host stats for sorting.
            # Ideally, LoadEvaluator provides a method to get hosts sorted by load for a given metric.
            # For now, we'll iterate hosts and pick VMs, then find a less loaded target.

            # Create a flat list of problematic resources (e.g. ['cpu', 'memory'])
            problematic_resources_names = [res for res, det in imbalance_details.items() if det.get('is_imbalanced')]
            if not problematic_resources_names:
                 logger.info("[MigrationPlanner_Balance] evaluate_imbalance returned details, but no specific resource marked as imbalanced. Skipping balancing moves.")
            else:
                logger.info(f"[MigrationPlanner_Balance] Problematic resources: {problematic_resources_names}")

                # Iterate through hosts, from most loaded to least (conceptual sorting)
                # For simplicity, iterate all hosts and try to move VMs if they are on "overloaded" side of the imbalance
                # This part needs more sophisticated source host selection based on `imbalance_details`
                
                # Get all host utilization % from LoadEvaluator
                host_resource_percentages = {} # {host_name: {'cpu': %, 'memory': %}, ...}
                # This requires a new method in LoadEvaluator or direct calculation here.
                # Example:
                _cpu_p, _mem_p, _disk_p, _net_p = self.load_evaluator.get_resource_percentage_lists()
                _host_names = [h.name for h in all_hosts_objects if hasattr(h, 'name')]
                for i, hn in enumerate(_host_names):
                    host_resource_percentages[hn] = {
                        'cpu': _cpu_p[i] if i < len(_cpu_p) else 0,
                        'memory': _mem_p[i] if i < len(_mem_p) else 0,
                        'disk': _disk_p[i] if i < len(_disk_p) else 0,
                        'network': _net_p[i] if i < len(_net_p) else 0,
                    }


                # Attempt to move VMs from hosts that are on the "high" side of any imbalanced resource
                for source_host_obj in all_hosts_objects:
                    if not hasattr(source_host_obj, 'name'): continue
                    source_host_metrics_pct = host_resource_percentages.get(source_host_obj.name, {})
                    
                    move_reason_details = [] # Store why we might move from this host

                    for res_name in problematic_resources_names:
                        res_threshold = self.load_evaluator.get_thresholds(self.aggressiveness).get(res_name)
                        # Check if this host is above average or contributing to max side of imbalance for 'res_name'
                        # This logic needs to be more precise: identify if host is a "max" host for 'res_name'
                        # For now, a simple check if it's above threshold (which is a diff, not a usage pct)
                        # A better check: if host's usage of res_name is part of the 'max_val' in imbalance_details[res_name]['max_val']
                        # For now, let's assume any host significantly above average for an imbalanced metric is a candidate source
                        # A simpler heuristic: if a host's usage for an imbalanced resource is high.
                        avg_usage_for_res = imbalance_details.get(res_name, {}).get('avg_val', 50) # Default to 50 if no avg
                        if source_host_metrics_pct.get(res_name, 0) > avg_usage_for_res + (res_threshold / 2.0): # If host is above avg + half_threshold
                             move_reason_details.append(f"high {res_name} ({source_host_metrics_pct.get(res_name,0):.1f}%)")


                    if not move_reason_details:
                        continue # This host is not a primary source for balancing moves

                    logger.info(f"[MigrationPlanner_Balance] Host '{source_host_obj.name}' is a candidate source for balancing. Reasons: {', '.join(move_reason_details)}")
                    
                    # Prioritize moving VMs contributing to the most imbalanced resource on this host
                    # For now, _select_vms_to_move can take a general imbalanced_resource hint
                    # Select one of the problematic_resources_names as a hint, e.g. the first one
                    # A more advanced selection would iterate through problematic_resources_names.
                    resource_hint_for_selection = problematic_resources_names[0] if problematic_resources_names else None

                    candidate_vms_to_move = self._select_vms_to_move(source_host_obj, resource_hint_for_selection, vms_in_migration_plan)

                    for vm_to_move in candidate_vms_to_move:
                        if vm_to_move.name in vms_in_migration_plan:
                            logger.debug(f"[MigrationPlanner_Balance] VM '{vm_to_move.name}' is already planned. Skipping for balancing.")
                            continue

                        # Pass only the problematic resource details to find_better_host
                        active_imbalance_details_for_host_finding = {
                             k: v for k,v in imbalance_details.items() if k in problematic_resources_names and v.get('is_imbalanced')
                        }
                        if not active_imbalance_details_for_host_finding:
                             logger.debug(f"No active imbalance details to guide host finding for {vm_to_move.name}. Skipping.")
                             continue


                        target_host_obj = self._find_better_host_for_balancing(vm_to_move, source_host_obj, all_hosts_objects, active_imbalance_details_for_host_finding)
                        
                        if target_host_obj:
                            migrations.append({'vm': vm_to_move, 'target_host': target_host_obj, 'reason': f"Resource Balancing ({', '.join(move_reason_details)})"})
                            vms_in_migration_plan.add(vm_to_move.name)
                            logger.info(f"[MigrationPlanner_Balance] Planned Balancing Migration: Move VM '{vm_to_move.name}' from '{source_host_obj.name}' to '{target_host_obj.name}'.")
                            # Simulate this move for subsequent decisions in this planning cycle if needed (complex)
                            # For now, assume LoadEvaluator's state is static for this planning cycle
                        else:
                            logger.info(f"[MigrationPlanner_Balance] No suitable balancing target found for VM '{vm_to_move.name}' from host '{source_host_obj.name}'.")
        else:
            logger.info("[MigrationPlanner_Balance] Cluster is already balanced. No resource balancing migrations needed.")

        if not migrations:
            logger.info("[MigrationPlanner] No migrations planned in this cycle.")
        else:
            logger.info(f"[MigrationPlanner] Total migrations planned: {len(migrations)}")
            for i, mig_plan in enumerate(migrations):
                logger.info(f"  {i+1}. VM: {mig_plan['vm'].name}, Target: {mig_plan['target_host'].name}, Reason: {mig_plan['reason']}")
        
        # Return unique plans (VMs should only be in one plan, ensured by vms_in_migration_plan set)
        # Convert dicts to tuples for `execute_migrations` if it expects (VM, TargetHost)
        final_migration_tuples = [(plan['vm'], plan['target_host']) for plan in migrations]
        return final_migration_tuples


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
                # Actual migration logic (e.g., API calls to vCenter/oVirt) would be here.
                # For simulation, we update cluster_state.
                
                # Simulate:
                # 1. Remove VM from source host's list of VMs
                # 2. Add VM to target host's list of VMs
                # 3. Update VM's own host reference
                # This should be done via ClusterState methods ideally.
                # self.cluster_state.move_vm(vm_obj, target_host_obj)
                
                logger.success(f"SUCCESS: Migration of '{vm_obj.name}' from '{source_host_name}' to '{target_host_obj.name}' completed (simulated).")
                # Potentially track event via a global/passed-in event tracker
            except Exception as e:
                logger.error(f"FAILED: Migration of '{vm_obj.name}' from '{source_host_name}' to '{target_host_obj.name}' failed: {str(e)}")
                # Potentially track event

# Helper method for LoadEvaluator (or make it part of LoadEvaluator class)
# This is a placeholder, assuming LoadEvaluator might need such a method
# or MigrationPlanner computes it.
# For now, _find_better_host_for_balancing needs this.
# This should ideally be in LoadEvaluator.
def get_resource_percentage_lists_for_host_placeholder(load_evaluator_instance, host_name):
    # This is a mock. Real implementation would get this from load_evaluator
    cpu_p, mem_p, disk_p, net_p = load_evaluator_instance.get_resource_percentage_lists()
    all_hosts = load_evaluator_instance.hosts # Assuming these are host objects or dicts with 'name'
    try:
        host_idx = [h.name if hasattr(h, 'name') else h.get('name') for h in all_hosts].index(host_name)
        return {
            'cpu': cpu_p[host_idx] if host_idx < len(cpu_p) else 0,
            'memory': mem_p[host_idx] if host_idx < len(mem_p) else 0,
            'disk': disk_p[host_idx] if host_idx < len(disk_p) else 0,
            'network': net_p[host_idx] if host_idx < len(net_p) else 0,
        }
    except (ValueError, IndexError):
        logger.warning(f"[get_resource_percentage_lists_for_host_placeholder] Host '{host_name}' not found in LoadEvaluator's host list.")
        return {}

# Monkey patch it into LoadEvaluator instances for the purpose of this exercise if not present
# In a real scenario, this method would be part of the LoadEvaluator class itself.
# from modules.load_evaluator import LoadEvaluator
# LoadEvaluator.get_resource_percentage_lists_for_host = get_resource_percentage_lists_for_host_placeholder
# This monkey patching is tricky here because LoadEvaluator is in another file.
# For now, assume that _find_better_host_for_balancing will have to compute this itself or
# that LoadEvaluator has been updated separately.
# The current _find_better_host_for_balancing calls self.load_evaluator.get_resource_percentage_lists_for_host(target_host_obj.name)
# This implies LoadEvaluator class needs this method.
# For the sake_of_this_subtask, I will assume such method is added to LoadEvaluator.
# If not, the call in _find_better_host_for_balancing needs to be adapted.
# For now, I've added a local copy of the host_resource_percentages calculation in plan_migrations itself.
# _find_better_host_for_balancing can use that if passed, or rely on a (not-yet-implemented in this script) LoadEvaluator method.
# Let's refine _find_better_host_for_balancing to take host_target_metrics_pct directly.

# Re-defining _find_better_host_for_balancing to accept target_metrics_pct to avoid dependency on un-added LoadEvaluator method
MigrationManager._find_better_host_for_balancing_orig = MigrationManager._find_better_host_for_balancing
def _find_better_host_for_balancing_revised(self, vm_to_move, source_host_obj, all_hosts, imbalanced_resources_details, host_resource_percentages_map):
    logger.debug(f"[MigrationPlanner] Finding better host for VM '{vm_to_move.name}' from '{source_host_obj.name}' for balancing (revised).")
    potential_targets = []

    for target_host_obj in all_hosts:
        if not hasattr(target_host_obj, 'name') or target_host_obj.name == source_host_obj.name:
            continue

        if not self._would_fit_on_host(vm_to_move, target_host_obj):
            continue

        if not self._is_anti_affinity_safe(vm_to_move, target_host_obj):
            continue

        score = 0
        target_metrics_pct = host_resource_percentages_map.get(target_host_obj.name)
        
        if not target_metrics_pct:
             logger.warning(f"Could not get metrics for target {target_host_obj.name} from provided map, skipping.")
             continue

        for resource, detail in imbalanced_resources_details.items():
             if resource in target_metrics_pct:
                 score += (100 - target_metrics_pct[resource]) 
        
        if score > 0:
            potential_targets.append({'host': target_host_obj, 'score': score})
    
    if not potential_targets:
        logger.info(f"[MigrationPlanner_FindBetterHost] No suitable balancing target host found for VM '{vm_to_move.name}'.")
        return None

    potential_targets.sort(key=lambda x: x['score'], reverse=True)
    best_target = potential_targets[0]['host']
    logger.info(f"[MigrationPlanner_FindBetterHost] Best balancing target for VM '{vm_to_move.name}' is '{best_target.name}' with score {potential_targets[0]['score']}.")
    return best_target
MigrationManager._find_better_host_for_balancing = _find_better_host_for_balancing_revised

# Ensure the main plan_migrations uses the revised call
# The main `plan_migrations` already calculates `host_resource_percentages` map.
# It needs to pass this map to `_find_better_host_for_balancing`.
# Let's adjust the call inside plan_migrations:
# target_host_obj = self._find_better_host_for_balancing(vm_to_move, source_host_obj, all_hosts_objects, active_imbalance_details_for_host_finding, host_resource_percentages)
# This change is made in the code above.

```
