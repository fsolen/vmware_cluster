
            __________________  _____ 
            |  ___|  _  \ ___ \/  ___|
            | |_  | | | | |_/ /\ `--. 
            |  _| | | | |    /  `--. \
            | |   | |/ /| |\ \ /\__/ /
            \_|   |___/ \_| \_|\____/ 
                                
    F D R S - Fully Dynamic Resource Scheduler

---

FDRS (Fully Dynamic Resource Scheduler) is a Python-based tool designed to automate and optimize resource management in VMware (by Broadcom) vSphere environments. It provides intelligent auto-balancing of cluster workloads and enforcement of VM anti-affinity rules to enhance performance and stability.

### Features

- **Cluster Auto Balancing**:
    - Dynamically balances clusters based on CPU, Memory, Network I/O, and Disk I/O metrics.
    - The goal is to keep the percentage point difference between the most and least utilized hosts (for each selected metric) within defined limits.
    - These limits are controlled by the `--aggressiveness` flag:
        - Level 5: Max 5% difference
        - Level 4: Max 10% difference
        - Level 3 (Default): Max 15% difference
        - Level 2: Max 20% difference
        - Level 1: Max 25% difference
- **Smart Anti-Affinity Rules**:
    - Automatically distributes VMs based on their names to improve resilience and performance.
    - VMs with the same name prefix (e.g., "webserver" derived from "webserver01", "webserver02" or "webserver123") are considered part of an anti-affinity group.
    - The rule ensures that the number of these sibling VMs on any single host does not differ by more than 1 from the count on any other host in the cluster.
- **Cron Support**: The CLI tool can be scheduled with cron jobs for automated execution.
- **Support for VMware vSphere**: Designed to work seamlessly with VMware vSphere Standard licensed clusters (DRS not required for FDRS functionality).
- **Dry-Run Mode**: Allows users to preview planned migrations without executing them, ensuring safety and control.
- **Max Migration**: Allow to control migration count per run by the `--max-migrations` flag (Default: 20 )
---

### Key Concepts

- **Workflow Priority**: FDRS processes rules in a specific order.
    1.  **Anti-Affinity First**: It first evaluates and plans migrations to satisfy anti-affinity rules.
    2.  **Resource Balancing**: After anti-affinity considerations, it evaluates the cluster for resource imbalances (CPU, Memory, Disk I/O, Network Throughput) and plans further migrations if necessary.
- **VM Grouping (Anti-Affinity)**: For anti-affinity, VMs are grouped based on their name prefix. The prefix is determined by removing the last numerical characters of the VM name.
- **Balancing Mechanism (Resource Balancing)**: Resource balancing aims to ensure that for any given metric (CPU, Memory, Disk I/O, Network Throughput), the difference in utilization percentage between the most loaded host and the least loaded host does not exceed the threshold defined by the chosen aggressiveness level.

---

### Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/fsolen/vsphere_scheduler.git
    cd vsphere_scheduler
    ```
2.  Ensure you have Python installed (Python 3.7+ is recommended).
3.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

---

## Example CLI Usage

**Note**: All examples require vCenter connection arguments: `--vcenter <vc_ip_or_hostname> --username <user> --password <pass>`.

### Default Behavior (Anti-Affinity and Balancing)

Runs the full FDRS workflow: first applies anti-affinity rules, then performs resource balancing using default aggressiveness (Level 3) for all metrics.

```bash
python fdrs.py --vcenter <vc_ip_or_hostname> --username <user> --password <pass>
```

### Auto Balancing Only (Specific Metrics and Aggressiveness and Max Migration Limits)

Focuses only on balancing specified metrics (CPU and Memory in this example) with a specific aggressiveness level (Level 4: Max 10% difference). Anti-affinity rules are not specifically enforced in this mode beyond what `MigrationManager` might consider for placement safety if it were extended to do so.

```bash
python fdrs.py --vcenter <vc_ip_or_hostname> --username <user> --password <pass> --balance --metrics cpu,memory --aggressiveness 4 --max-migrations 50
```

### Apply Anti-Affinity Rules Only

This command *only* evaluates and enforces anti-affinity rules, making any necessary migrations to satisfy them. Resource balancing is not performed.

```bash
python fdrs.py --vcenter <vc_ip_or_hostname> --username <user> --password <pass> --apply-anti-affinity
```

### Dry Run (Simulate Changes)

To see what migrations FDRS would perform without actually making any changes, add the `--dry-run` flag to any command:

```bash
python fdrs.py --vcenter <vc_ip_or_hostname> --username <user> --password <pass> --dry-run
```

### Schedule with Cron

Schedule the default FDRS workflow (anti-affinity + balancing) to run daily at midnight and log output. Adjust paths as necessary.

```bash
0 0 * * * /usr/bin/python /path/to/your/cloned/repo/vmware_cluster/fdrs.py --vcenter <vc_ip_or_hostname> --username <user> --password <pass> >> /var/log/fdrs.log 2>&1
```

### Help Command

For a full list of options and detailed explanations:

```bash
python fdrs.py --help
```

---

## Roadmap

*   FDSS (Fully Dynamic Storage Scheduler): Potential future development for storage-specific dynamic scheduling.
*             VMFS datastore anti-affinity group logic with namimg pattern
*             VMFS IO performance balancing
*   Enhanced vSphere API Integration: Exploring deeper integration with VMware APIs for more advanced features and metrics.
*             Cluster name switch
*             Ignore anti-affinity switch
*             More secure authentication
*             Select best host and best datastore switch awareness with anti-affinity and performance
