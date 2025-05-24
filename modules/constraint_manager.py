import logging

logger = logging.getLogger('fdrs')

class ConstraintManager:
    def __init__(self, cluster_state):
        self.cluster_state = cluster_state
        self.vm_distribution = {}

    def enforce_anti_affinity(self):
        """
        Automatically groups VMs by prefix (ignoring last 2 chars)
        and tries to ensure VMs with same prefix are spread across hosts.
        """
        logger.info("[ConstraintManager] Enforcing automatic Anti-Affinity rules...")
        self.vm_distribution = {}

        # Build groups of VMs by common prefix
        for vm in self.cluster_state.vms:
            short_name = vm.name[:-2]  # Remove last 2 characters
            if short_name not in self.vm_distribution:
                self.vm_distribution[short_name] = []
            self.vm_distribution[short_name].append(vm)

        logger.debug("[ConstraintManager] Grouped VMs by prefix: {}".format(
            {k: [vm.name for vm in vms] for k, vms in self.vm_distribution.items()}
        ))

    def validate_anti_affinity(self):
        """
        Validate if VMs sharing prefix are located on different hosts.
        Returns list of VM names violating the rule.
        """
        logger.info("[ConstraintManager] Validating Anti-Affinity violations...")
        violations = []

        for prefix, vms in self.vm_distribution.items():
            host_set = set()
            vm_by_host = {}
            for vm in vms:
                host_name = self.cluster_state.get_host_of_vm(vm)
                if not host_name:
                    continue
                if host_name in host_set:
                    # This is a violation - same prefix VMs on same host
                    violations.append(vm.name)
                    if host_name in vm_by_host:
                        # Also add the other VM that was first found on this host
                        violations.append(vm_by_host[host_name].name)
                else:
                    host_set.add(host_name)
                    vm_by_host[host_name] = vm

        if not violations:
            logger.info("[ConstraintManager] No Anti-Affinity violations detected.")
        else:
            logger.info("[ConstraintManager] Found {} Anti-Affinity violations.".format(len(violations)))

        return list(set(violations))  # Remove duplicates

    def get_preferred_host_for_vm(self, vm):
        """
        Given a VM, suggest a host to move it based on Anti-Affinity rules.
        Prefer a host not hosting same-prefix siblings.
        """
        short_name = vm.name[:-2]
        sibling_vms = self.vm_distribution.get(short_name, [])

        # Get list of hosts already hosting siblings
        occupied_hosts = set()
        for sibling in sibling_vms:
            if sibling.name == vm.name:
                continue  # skip itself
            host = self.cluster_state.get_host_of_vm(sibling)
            if host:
                occupied_hosts.add(host)

        # Suggest a host not in occupied_hosts
        for host in self.cluster_state.hosts:
            if host.name not in occupied_hosts:
                return host

        logger.warning("[ConstraintManager] No free host found for VM '{}' to avoid Anti-Affinity violation.".format(vm.name))
        return None  # fallback if no better host found
        
    def apply(self):
        """
        Apply anti-affinity rules (enforce and validate).
        """
        self.enforce_anti_affinity()
        self.validate_anti_affinity()
