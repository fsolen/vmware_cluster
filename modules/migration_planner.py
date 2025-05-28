from modules.logger import Logger

logger = Logger()

class MigrationManager:
    def __init__(self, cluster_state, constraint_manager, aggressiveness=3):
        self.cluster_state = cluster_state
        self.constraint_manager = constraint_manager
        self.aggressiveness = aggressiveness
        self.logger = Logger()  # Use our enhanced Logger

    def plan_migrations(self):
        """
        Main function to decide which VMs should move where.
        Returns a list of (VM, TargetHost) tuples.
        """
        self.logger.info("[MigrationPlanner] Planning migrations...")
        migrations = []

        imbalance_hosts = self._detect_imbalanced_hosts()

        # Handle Anti-Affinity violations first
        anti_affinity_violations = self.constraint_manager.validate_anti_affinity()
        for vm_name in anti_affinity_violations:
            vm = self.cluster_state.get_vm_by_name(vm_name)
            if hasattr(vm, 'config') and getattr(vm.config, 'template', False):
                self.logger.info(f"[MigrationPlanner] Skipping template VM '{vm.name}' in planning phase")
                continue
            preferred_host = self.constraint_manager.get_preferred_host_for_vm(vm)
            if preferred_host:
                migrations.append((vm, preferred_host))
                self.logger.track_migration(
                    vm_name=vm.name,
                    source_host=self.cluster_state.get_host_of_vm(vm),
                    target_host=preferred_host.name,
                    reason="Anti-Affinity rule violation",
                    metrics={
                        'source_metrics': self.cluster_state.host_metrics.get(self.cluster_state.get_host_of_vm(vm), {}),
                        'target_metrics': self.cluster_state.host_metrics.get(preferred_host.name, {}),
                        'vm_metrics': self.cluster_state.vm_metrics.get(vm.name, {})
                    }
                )
                self.logger.info(f"[MigrationPlanner] Anti-Affinity fix planned: Move '{vm.name}' ➔ '{preferred_host.name}'")

        # Handle Load imbalance
        for host in imbalance_hosts:
            overloaded_vms = self._select_vms_to_move(host)
            for vm in overloaded_vms:
                if hasattr(vm, 'config') and getattr(vm.config, 'template', False):
                    self.logger.info(f"[MigrationPlanner] Skipping template VM '{vm.name}' in planning phase")
                    continue
                target_host = self._find_better_host(vm, current_host=host)
                if target_host:
                    migrations.append((vm, target_host))
                    self.logger.track_migration(
                        vm_name=vm.name,
                        source_host=host.name,
                        target_host=target_host.name,
                        reason=f"Host overload (CPU: {self.cluster_state.host_metrics[host.name]['cpu_usage_pct']:.1f}%, MEM: {self.cluster_state.host_metrics[host.name]['memory_usage_pct']:.1f}%)",
                        metrics={
                            'source_metrics': self.cluster_state.host_metrics.get(host.name, {}),
                            'target_metrics': self.cluster_state.host_metrics.get(target_host.name, {}),
                            'vm_metrics': self.cluster_state.vm_metrics.get(vm.name, {})
                        }
                    )
                    self.logger.info(f"[MigrationPlanner] Load fix planned: Move '{vm.name}' from '{host.name}' ➔ '{target_host.name}'")
        
        if not migrations:
            self.logger.info("[MigrationPlanner] No migrations needed. Cluster is healthy.")

        return migrations

    def execute_migrations(self, migrations):
        """Execute the planned migrations"""
        for vm, target_host in migrations:
            try:
                # Here would be the actual migration code...
                source_host = self.cluster_state.get_host_of_vm(vm)
                self.logger.success(f"Migration of '{vm.name}' from '{source_host}' to '{target_host.name}' completed")
                self.logger.track_event('migration_complete', {
                    'vm_name': vm.name,
                    'source_host': source_host,
                    'target_host': target_host.name,
                    'success': True
                })
            except Exception as e:
                self.logger.error(f"Migration of '{vm.name}' failed: {str(e)}")
                self.logger.track_event('migration_complete', {
                    'vm_name': vm.name,
                    'source_host': source_host,
                    'target_host': target_host.name,
                    'success': False,
                    'error': str(e)
                })

    def _detect_imbalanced_hosts(self):
        """
        Find hosts that are overloaded based on CPU, Memory, Disk IO, Network IO.
        Aggressiveness factor (1-5) controls sensitivity.
        """
        overloaded_hosts = []
        self.logger.info("[MigrationPlanner] Detecting overloaded hosts...")
        for host in self.cluster_state.hosts:
            metrics = self.cluster_state.host_metrics.get(host.name, {})
            if self._is_overloaded(metrics):
                overloaded_hosts.append(host)
                self.logger.info(f"[MigrationPlanner] Host '{host.name}' is overloaded:")
                self.logger.info(f"  CPU: {metrics.get('cpu_usage_pct', 0):.1f}%")
                self.logger.info(f"  Memory: {metrics.get('memory_usage_pct', 0):.1f}%")
                self.logger.info(f"  Disk I/O: {metrics.get('disk_io_usage', 0):.1f} MBps")
                self.logger.info(f"  Network I/O: {metrics.get('network_io_usage', 0):.1f} MBps")

        return overloaded_hosts

    def _is_overloaded(self, metrics):
        """
        Determine if a host is overloaded.
        Thresholds become stricter with higher aggressiveness.
        """
        cpu_threshold = 70 - (self.aggressiveness * 5)  # eg. aggressiveness 3 => 55%
        mem_threshold = 75 - (self.aggressiveness * 5)
        disk_threshold = 80 - (self.aggressiveness * 5)
        net_threshold = 80 - (self.aggressiveness * 5)

        return (
            metrics.get('cpu_usage_pct', 0.0) > cpu_threshold or
            metrics.get('memory_usage_pct', 0.0) > mem_threshold or
            metrics.get('disk_io_usage', 0.0) > disk_threshold or
            metrics.get('network_io_usage', 0.0) > net_threshold
        )

    def _select_vms_to_move(self, host):
        """
        From an overloaded host, pick a few VMs that are heavy (CPU, Memory, IO).
        """
        # Get VMs running on this host
        vms_on_host = [vm for vm in self.cluster_state.vms if self.cluster_state.get_host_of_vm(vm) == host.name]
        heavy_vms = sorted(
            vms_on_host,
            key=lambda vm: (
                getattr(vm, 'cpu_usage', 0) + getattr(vm, 'memory_usage', 0) + getattr(vm, 'disk_io_usage', 0) + getattr(vm, 'network_io_usage', 0)
            ),
            reverse=True
        )
        # Pick top 1-3 heavy VMs depending on aggressiveness
        count = min(self.aggressiveness, len(heavy_vms))
        return heavy_vms[:count]

    def _find_better_host(self, vm, current_host):
        """
        Search for a better host for the given VM.
        """
        candidates = []

        for host in self.cluster_state.hosts:
            if host.name == current_host.name:
                continue  # Skip current host
            if self._would_fit(vm, host):
                score = self._score_host_for_vm(vm, host)
                candidates.append((score, host))

        if not candidates:
            logger.warning(f"[MigrationPlanner] No better host found for VM '{vm.name}'.")
            return None

        # Pick host with best score
        candidates.sort(reverse=True)
        best_host = candidates[0][1]
        return best_host

    def _would_fit(self, vm, host):
        """
        Check if a VM would fit on a target host based on resource constraints.
        Returns True if the VM can be placed on the host without overloading it.
        """
        host_metrics = self.cluster_state.host_metrics.get(host.name, {})
        vm_metrics = self.cluster_state.vm_metrics.get(vm.name, {})
        
        # Calculate projected resource usage
        projected_cpu = host_metrics.get('cpu_usage', 0) + vm_metrics.get('cpu_usage', 0)
        projected_mem = host_metrics.get('memory_usage', 0) + vm_metrics.get('memory_usage', 0)
        projected_disk = host_metrics.get('disk_io_usage', 0) + vm_metrics.get('disk_io_usage', 0)
        projected_net = host_metrics.get('network_io_usage', 0) + vm_metrics.get('network_io_usage', 0)
        
        # Define thresholds based on aggressiveness (lower = more conservative)
        cpu_threshold = 80 - (self.aggressiveness * 5)  # 65-75% depending on aggressiveness
        mem_threshold = 85 - (self.aggressiveness * 5)  # 70-80%
        io_threshold = 70 - (self.aggressiveness * 5)   # 55-65%
        
        # Check if projected usage is within thresholds
        if (projected_cpu > cpu_threshold or
            projected_mem > mem_threshold or
            projected_disk > io_threshold or
            projected_net > io_threshold):
            return False
            
        return True

    def _score_host_for_vm(self, vm, host):
        """
        Score a host as a target for VM placement.
        Returns a score where higher is better.
        Takes into account:
        - Current resource utilization
        - Resource balance
        - Anti-affinity rules
        """
        if not self._would_fit(vm, host):
            return float('-inf')  # Host can't accommodate VM
            
        host_metrics = self.cluster_state.host_metrics.get(host.name, {})
        base_score = 100
        
        # Penalize based on current resource utilization
        cpu_penalty = host_metrics.get('cpu_usage', 0) * 0.4
        mem_penalty = host_metrics.get('memory_usage', 0) * 0.3
        io_penalty = (host_metrics.get('disk_io_usage', 0) + 
                     host_metrics.get('network_io_usage', 0)) * 0.15
        
        # Check anti-affinity rules if constraint manager is available
        affinity_penalty = 0
        if self.constraint_manager:
            vm_prefix = vm.name[:-2]  # Remove last 2 chars
            for other_vm in self.cluster_state.get_vms_on_host(host):
                if other_vm.name[:-2] == vm_prefix:
                    affinity_penalty = 50  # Heavy penalty for anti-affinity violation
                    
        # Calculate final score
        score = base_score - cpu_penalty - mem_penalty - io_penalty - affinity_penalty
        
        return max(0, score)  # Don't return negative scores
