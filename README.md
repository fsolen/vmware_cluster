
            __________________  _____ 
            |  ___|  _  \ ___ \/  ___|
            | |_  | | | | |_/ /\ `--. 
            |  _| | | | |    /  `--. \
            | |   | |/ /| |\ \ /\__/ /
            \_|   |___/ \_| \_|\____/ 
                                
    F D R S - Fully Dynamic Resource Scheduler

---

### Features

- **Cluster Auto Balancing**: Dynamically balances clusters based on CPU, Memory, Network, and Disk IO metrics.
- **Anti-Affinity Rules**: Automatically applies anti-affinity rules by analyzing VM inventory names to improve performance.
- **Cron Support**: The CLI tool can be scheduled with cron jobs for automated execution.
- **Support for VMware vSphere**: Designed to work seamlessly with VMware vSphere standard clusters.

---

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/fsolen/vmware_cluster.git
   cd vmware_cluster
   ```

2. Ensure you have Python installed (Python 3.7+ is recommended).
3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```
## Example CLI Usage
### Auto Balancing

Run the tool to auto-balance your cluster based on CPU and Memory usage:

   ```bash
   python fdrs.py --balance --metrics cpu,memory
   ```

### Apply Anti-Affinity Rules
Automatically generate and apply rules for VMs in the cluster:

   ```bash
   python fdrs.py --apply-anti-affinity
   ```
### Schedule with Cron
Schedule the tool to run every day at midnight via cron:

   ```bash
   python fdrs.py --apply-anti-affinity
   ```
### Help Command
For more options and details:

   ```bash
   python fdrs.py --help
   ```
## Roadmap
* FDSS (Fully Dynamic Storage Scheduler): Currently under development to provide storage-specific dynamic scheduling.
* Integration with advanced VMware APIs to enhance functionality.

