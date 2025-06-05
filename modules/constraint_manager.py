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
            
            name_part = vm.name.rstrip('0123456789')
            if name_part: # Ensure rstrip didn't leave an empty string
                short_name = name_part
            else: 
                short_name = vm.name # Use original name if rstrip results in empty (e.g. name is "123")

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
        for vm_in_group_iter in vms_in_group: # Renamed from vm_in_group_iter_pre_calc
            h_iter = self.cluster_state.get_host_of_vm(vm_in_group_iter) # Renamed from h_iter_pre_calc
            if h_iter and hasattr(h_iter, 'name') and h_iter.name in current_host_group_counts:
                current_host_group_counts[h_iter.name] += 1

        # Try to find a host that achieves perfect balance
        logger.info(f"[ConstraintManager] Attempting to find a 'perfect balance' host for VM '{vm_to_move.name}'.")
        best_target_host_obj = self._find_perfect_balance_host(vm_to_move, current_host_group_counts, source_host_name, active_hosts)

        if best_target_host_obj:
            logger.info(f"[ConstraintManager] Found 'perfect balance' host '{best_target_host_obj.name}' for VM '{vm_to_move.name}'.")
            return best_target_host_obj

        # If no perfect balance host, try to find a host that is better than the source
        logger.info(f"[ConstraintManager] No 'perfect balance' host found for VM '{vm_to_move.name}'. Attempting to find a 'better than source' host.")
        source_host_group_count = current_host_group_counts.get(source_host_name, float('inf'))
        best_target_host_obj = self._find_better_than_source_host(vm_to_move, current_host_group_counts, source_host_name, source_host_group_count, active_hosts)

        if best_target_host_obj:
            logger.info(f"[ConstraintManager] Found 'better than source' host '{best_target_host_obj.name}' for VM '{vm_to_move.name}'.")
        else:
            logger.warning(f"[ConstraintManager] No suitable host found for VM '{vm_to_move.name}' using either strategy.")
        return best_target_host_obj

    def _find_perfect_balance_host(self, vm_to_move, current_host_group_counts, source_host_name, active_hosts):
        '''
        Finds a host that, if the VM were moved to it, would result in "perfect"
        anti-affinity balance (max_count - min_count <= 1 for the group).
        '''
        best_target_host_obj = None
        perfect_balance_candidates = []

        for target_host_obj in active_hosts:
            if not hasattr(target_host_obj, 'name'): continue
            target_host_name = target_host_obj.name
            # Ensure source host is not considered as a target
            if target_host_name == source_host_name:
                continue

            # Simulate the move
            simulated_host_vm_counts = current_host_group_counts.copy()
            simulated_host_vm_counts[source_host_name] = simulated_host_vm_counts.get(source_host_name, 1) - 1
            simulated_host_vm_counts[target_host_name] = simulated_host_vm_counts.get(target_host_name, 0) + 1
            
            sim_counts_values = [simulated_host_vm_counts[h.name] for h in active_hosts if hasattr(h, 'name') and h.name in simulated_host_vm_counts]
            if not sim_counts_values: continue # Should not happen if active_hosts is not empty

            sim_min_count = min(sim_counts_values)
            sim_max_count = max(sim_counts_values)

            if sim_max_count - sim_min_count <= 1:
                perfect_balance_candidates.append(target_host_obj)

        if perfect_balance_candidates:
            lowest_target_host_group_vm_count = float('inf')
            # Select the best candidate from the perfect balance list
            for candidate_host_obj in perfect_balance_candidates:
                candidate_host_name = candidate_host_obj.name
                current_count_on_candidate = current_host_group_counts.get(candidate_host_name, 0)
                if current_count_on_candidate < lowest_target_host_group_vm_count:
                    lowest_target_host_group_vm_count = current_count_on_candidate
                    best_target_host_obj = candidate_host_obj
                elif current_count_on_candidate == lowest_target_host_group_vm_count:
                    # Tie-breaking: prefer host with lexicographically smaller name
                    if best_target_host_obj and hasattr(best_target_host_obj, 'name') and candidate_host_obj.name < best_target_host_obj.name:
                        best_target_host_obj = candidate_host_obj
                    elif not best_target_host_obj:
                        best_target_host_obj = candidate_host_obj
            logger.debug(f"[ConstraintManager] Perfect balance candidates for VM '{vm_to_move.name}': {[h.name for h in perfect_balance_candidates]}. Selected: {best_target_host_obj.name if best_target_host_obj else 'None'}")
        return best_target_host_obj

    def _find_better_than_source_host(self, vm_to_move, current_host_group_counts, source_host_name, source_host_group_count, active_hosts):
        '''
        Finds a host that has fewer VMs of the same group than the source host.
        This is a fallback if no "perfect balance" host is found.
        '''
        best_target_host_obj = None
        min_group_vms_on_target = float('inf')

        for target_host_obj in active_hosts:
            if not hasattr(target_host_obj, 'name'): continue
            target_host_name = target_host_obj.name
            # Ensure source host is not considered as a target
            if target_host_name == source_host_name:
                continue
            
            current_count_on_target_for_group = current_host_group_counts.get(target_host_name, 0)

            # Check if this target is better than the source host
            if current_count_on_target_for_group < source_host_group_count:
                if current_count_on_target_for_group < min_group_vms_on_target:
                    min_group_vms_on_target = current_count_on_target_for_group
                    best_target_host_obj = target_host_obj
                elif current_count_on_target_for_group == min_group_vms_on_target:
                    # Tie-breaking: prefer host with lexicographically smaller name
                    if best_target_host_obj and hasattr(best_target_host_obj, 'name') and target_host_obj.name < best_target_host_obj.name:
                        best_target_host_obj = target_host_obj
                    elif not best_target_host_obj:
                         best_target_host_obj = target_host_obj
        
        if best_target_host_obj:
            logger.debug(f"[ConstraintManager] Better than source host candidates for VM '{vm_to_move.name}'. Selected: {best_target_host_obj.name}")
        else:
            logger.debug(f"[ConstraintManager] No host found better than source for VM '{vm_to_move.name}'.")
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
