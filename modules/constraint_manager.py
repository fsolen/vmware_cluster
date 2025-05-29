import logging

logger = logging.getLogger('fdrs')

class ConstraintManager:
    def __init__(self, cluster_state):
        self.cluster_state = cluster_state
        self.vm_distribution = {}
        self.violations = [] # Store identified violations

    def enforce_anti_affinity(self):
        # Docstring using single quotes to avoid issues
        '''
        Groups VMs by prefix (ignoring last 2 chars).
        This populates self.vm_distribution.
        '''
        logger.info("[ConstraintManager] Grouping VMs by prefix for Anti-Affinity rules...")
        self.vm_distribution = {}
        all_vms = self.cluster_state.vms # Use direct attribute

        if not all_vms:
            logger.info("[ConstraintManager] No VMs found in cluster state.")
            return

        for vm in all_vms:
            if not hasattr(vm, 'name') or len(vm.name) < 3: 
                logger.warning(f"[ConstraintManager] VM with invalid name or missing name attribute skipped: {getattr(vm, 'name', 'UnknownVM')}")
                continue
            
            short_name = vm.name[:-2]

            if short_name not in self.vm_distribution:
                self.vm_distribution[short_name] = []
            self.vm_distribution[short_name].append(vm)

        logger.debug(f"[ConstraintManager] Grouped VMs by prefix: {{k: [vm.name for vm in vms] for k, vms in self.vm_distribution.items()}}")

    def calculate_anti_affinity_violations(self):
        # Escaped internal double quotes
        """
        Calculates VM anti-affinity violations based on the rule:
        \"For VMs with the same prefix, the count of such VMs on any host
         should not differ by more than 1 from the count on any other host.\"
        Returns a list of VM objects that are on \"over-subscribed\" hosts for their group.
        """
        logger.info("[ConstraintManager] Calculating Anti-Affinity violations...")
        all_violations = []
        active_hosts = self.cluster_state.hosts # Use direct attribute

        if not active_hosts or len(active_hosts) <= 1:
            logger.info("[ConstraintManager] Not enough active hosts (<2) to apply anti-affinity distribution rules.")
            return []

        for prefix, vms_in_group in self.vm_distribution.items():
            if not vms_in_group:
                continue

            host_vm_counts = {host.name: 0 for host in active_hosts if hasattr(host, 'name')}
            vms_on_hosts_map = {host.name: [] for host in active_hosts if hasattr(host, 'name')}
            
            current_group_vms_on_hosts = 0
            for vm in vms_in_group:
                host = self.cluster_state.get_host_of_vm(vm) 
                if host and hasattr(host, 'name') and host.name in host_vm_counts:
                    host_vm_counts[host.name] += 1
                    vms_on_hosts_map[host.name].append(vm)
                    current_group_vms_on_hosts += 1
            
            if current_group_vms_on_hosts == 0:
                logger.debug(f"[ConstraintManager] No VMs from group '{prefix}' are currently on the monitored hosts.")
                continue

            actual_counts_for_active_hosts = [host_vm_counts[h.name] for h in active_hosts if hasattr(h, 'name') and h.name in host_vm_counts]
            if not actual_counts_for_active_hosts:
                logger.debug(f"[ConstraintManager] No VMs from group '{prefix}' have count > 0 on any active host.")
                continue

            min_count = min(actual_counts_for_active_hosts)
            max_count = max(actual_counts_for_active_hosts)

            if max_count - min_count > 1:
                logger.info(f"[ConstraintManager] Anti-Affinity violation for group '{prefix}'. Host counts for group: {host_vm_counts}")
                for host_name, count in host_vm_counts.items():
                    if count == max_count:
                        logger.debug(f"[ConstraintManager] VMs on host '{host_name}' (count: {count}) from group '{prefix}' are contributing to violation.")
                        all_violations.extend(vms_on_hosts_map[host_name])
        
        unique_violations = list(set(all_violations))
        logger.info(f"[ConstraintManager] Total unique anti-affinity violations found: {len(unique_violations)}")
        return unique_violations

    def get_preferred_host_for_vm(self, vm_to_move):
        '''
        Suggests a preferred host for 'vm_to_move' to resolve an anti-affinity violation.
        '''
        logger.debug(f"[ConstraintManager] Getting preferred host for VM '{vm_to_move.name}'")
        
        if not hasattr(vm_to_move, 'name') or len(vm_to_move.name) < 3:
            logger.warning(f"[ConstraintManager] Invalid vm_to_move object: {vm_to_move}")
            return None
        vm_prefix = vm_to_move.name[:-2]
        
        if not self.vm_distribution: 
            logger.info("[ConstraintManager] vm_distribution is empty, populating it first.")
            self.enforce_anti_affinity() 
            if not self.vm_distribution:
                 logger.warning(f"[ConstraintManager] vm_distribution still empty. Cannot determine preferred host for {vm_to_move.name}")
                 return None

        vms_in_group = self.vm_distribution.get(vm_prefix)
        if not vms_in_group:
            logger.warning(f"[ConstraintManager] VM '{vm_to_move.name}' has no group in vm_distribution (prefix: {vm_prefix}). Distribution keys: {list(self.vm_distribution.keys())}")
            return None

        source_host_obj = self.cluster_state.get_host_of_vm(vm_to_move)
        if not source_host_obj or not hasattr(source_host_obj, 'name'):
            logger.warning(f"[ConstraintManager] Cannot determine valid source host for VM '{vm_to_move.name}'.")
            return None
        source_host_name = source_host_obj.name

        active_hosts = self.cluster_state.hosts # Use direct attribute
        if not active_hosts or len(active_hosts) <= 1:
            logger.info("[ConstraintManager] Not enough active hosts to find a preferred host.")
            return None

        best_target_host_obj = None
        
        current_host_group_counts = {host.name: 0 for host in active_hosts if hasattr(host, 'name')}
        for vm_in_group_iter_pre_calc in vms_in_group:
            h_iter_pre_calc = self.cluster_state.get_host_of_vm(vm_in_group_iter_pre_calc)
            if h_iter_pre_calc and hasattr(h_iter_pre_calc, 'name') and h_iter_pre_calc.name in current_host_group_counts:
                current_host_group_counts[h_iter_pre_calc.name] += 1

        # Pass 1: Find hosts that achieve perfect balance
        perfect_balance_candidates = []
        for target_host_obj in active_hosts:
            if not hasattr(target_host_obj, 'name'): continue
            target_host_name = target_host_obj.name
            if target_host_name == source_host_name:
                continue

            simulated_host_vm_counts = current_host_group_counts.copy()
            simulated_host_vm_counts[source_host_name] = simulated_host_vm_counts.get(source_host_name, 1) - 1
            simulated_host_vm_counts[target_host_name] = simulated_host_vm_counts.get(target_host_name, 0) + 1
            
            sim_counts_values = [simulated_host_vm_counts[h.name] for h in active_hosts if hasattr(h, 'name') and h.name in simulated_host_vm_counts]
            if not sim_counts_values: continue

            sim_min_count = min(sim_counts_values)
            sim_max_count = max(sim_counts_values)

            if sim_max_count - sim_min_count <= 1:
                perfect_balance_candidates.append(target_host_obj)

        if perfect_balance_candidates:
            lowest_target_host_group_vm_count = float('inf')
            for candidate_host_obj in perfect_balance_candidates:
                candidate_host_name = candidate_host_obj.name # Assuming name attribute exists due to earlier checks
                current_count_on_candidate = current_host_group_counts.get(candidate_host_name, 0)
                if current_count_on_candidate < lowest_target_host_group_vm_count:
                    lowest_target_host_group_vm_count = current_count_on_candidate
                    best_target_host_obj = candidate_host_obj
                elif current_count_on_candidate == lowest_target_host_group_vm_count:
                    if best_target_host_obj and hasattr(best_target_host_obj, 'name') and candidate_host_obj.name < best_target_host_obj.name:
                        best_target_host_obj = candidate_host_obj
                    elif not best_target_host_obj: # Should only happen for the first candidate meeting the criteria
                        best_target_host_obj = candidate_host_obj
            logger.info(f"[ConstraintManager] Preferred host for VM '{vm_to_move.name}' (perfect balance) is '{best_target_host_obj.name}'.")
            return best_target_host_obj

        # Pass 2: If no perfect candidates, find host that minimally has fewer VMs of the group than source
        logger.info(f"[ConstraintManager] No host achieves perfect AA balance for VM '{vm_to_move.name}'. Trying to find host with fewer group VMs than source.")
        min_group_vms_on_target = float('inf')
        source_host_group_count = current_host_group_counts.get(source_host_name, float('inf'))

        for target_host_obj in active_hosts:
            if not hasattr(target_host_obj, 'name'): continue
            target_host_name = target_host_obj.name
            if target_host_name == source_host_name:
                continue
            
            current_count_on_target_for_group = current_host_group_counts.get(target_host_name, 0)

            if current_count_on_target_for_group < source_host_group_count:
                if current_count_on_target_for_group < min_group_vms_on_target:
                    min_group_vms_on_target = current_count_on_target_for_group
                    best_target_host_obj = target_host_obj
                elif current_count_on_target_for_group == min_group_vms_on_target:
                    if best_target_host_obj and hasattr(best_target_host_obj, 'name') and target_host_obj.name < best_target_host_obj.name:
                        best_target_host_obj = target_host_obj
                    elif not best_target_host_obj: # Should only happen for the first candidate in this pass
                        best_target_host_obj = target_host_obj
        
        if best_target_host_obj:
            logger.info(f"[ConstraintManager] No host achieves perfect AA balance. Selecting host '{best_target_host_obj.name}' to reduce load from source for VM '{vm_to_move.name}'.")
        else:
            logger.warning(f"[ConstraintManager] No suitable host found for VM '{vm_to_move.name}' even with relaxed anti-affinity criteria.")
        return best_target_host_obj

    def apply(self):
        '''
        Applies anti-affinity rules by first grouping VMs and then calculating violations.
        Violations are stored in self.violations.
        '''
        self.enforce_anti_affinity() 
        self.violations = self.calculate_anti_affinity_violations()

        if self.violations:
            logger.info(f"[ConstraintManager] Apply: Found {len(self.violations)} unique Anti-Affinity violations.")
        else:
            logger.info("[ConstraintManager] Apply: No Anti-Affinity violations detected.")
