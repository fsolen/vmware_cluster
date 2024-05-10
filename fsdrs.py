import ssl
import time
import yaml
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

class vCenterConnector:
    def __init__(self, config_file):
        self.config_file = config_file
        self.service_instance = None

    def connect(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)

        host = config['host']
        username = config['username']
        password = config['password']

        context = None
        if hasattr(ssl, "_create_unverified_context"):
            context = ssl._create_unverified_context()

        try:
            self.service_instance = SmartConnect(host=host,
                                                  user=username,
                                                  pwd=password,
                                                  sslContext=context)
            return True
        except Exception as e:
            print("Unable to connect to vCenter:", str(e))
            return False

    def disconnect(self):
        try:
            if self.service_instance:
                Disconnect(self.service_instance)
                print("Disconnected from vCenter")
        except Exception as e:
            print("Error disconnecting from vCenter:", str(e))

def get_vm_metrics(vcenter):
    vm_metrics = {}
    try:
        content = vcenter.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(content.rootFolder,
                                                                 [vim.VirtualMachine],
                                                                 True)
        vms = container_view.view

        for vm in vms:
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                summary = vm.summary
                cpu_usage = summary.quickStats.overallCpuUsage
                memory_usage = summary.quickStats.guestMemoryUsage
                vm_metrics[vm.name] = (cpu_usage, memory_usage)

        return vm_metrics
    except Exception as e:
        print(f"Error retrieving VM metrics: {str(e)}")
        return None

def get_host_metrics(vcenter):
    host_metrics = {}
    try:
        content = vcenter.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(content.rootFolder,
                                                                 [vim.HostSystem],
                                                                 True)
        hosts = container_view.view

        for host in hosts:
            summary = host.summary
            cpu_utilization = summary.quickStats.overallCpuUsage
            cpu_capacity = host.hardware.cpuInfo.hz / 1000000  # Convert Hz to MHz

            memory_utilization = summary.quickStats.overallMemoryUsage
            memory_capacity = host.hardware.memorySize / (1024 * 1024)  # Convert bytes to megabytes


            host_metrics[host.name] = (cpu_capacity, memory_capacity)

        return host_metrics
    except Exception as e:
        print(f"Error retrieving host metrics: {str(e)}")
        return None

def migrate_vm_to_host(vm_name, host_name, service_instance):
    try:
        content = service_instance.RetrieveContent()
        vm_obj = None
        host_obj = None

        # Find the VM object by name
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        for item in container.view:
            if item.name == vm_name:
                vm_obj = item
                break

        # Find the host object by name
        host_container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
        for item in host_container.view:
            if item.name == host_name:
                host_obj = item
                break

        # Perform VM migration
        if vm_obj and host_obj:
            task = vm_obj.Migrate(host=host_obj, priority=vim.VirtualMachine.MovePriority.highPriority)
            task_result = task.waitForTask()
            if task_result == vim.TaskInfo.State.success:
                print(f"Successfully migrated VM {vm_name} to host {host_name}")
            else:
                print(f"Failed to migrate VM {vm_name} to host {host_name}. Task result: {task_result}")
        else:
            print(f"Failed to find VM {vm_name} or host {host_name}")
    except Exception as e:
        print(f"Error migrating VM {vm_name} to host {host_name}: {str(e)}")

def main():
    vcenter_config_file = "vcenter01_config.yaml"
    vcenter_connector = vCenterConnector(vcenter_config_file)

    if vcenter_connector.connect():
        try:
            # Get initial host metrics
            host_metrics = get_host_metrics(vcenter_connector.service_instance)
            if not host_metrics:
                print("Failed to retrieve host metrics. Exiting.")
                return

            # Perform what-if analysis and migrate VMs one by one with a 1-minute interval
            vm_metrics = get_vm_metrics(vcenter_connector.service_instance)
            if not vm_metrics:
                print("Failed to retrieve VM metrics. Exiting.")
                return

            while vm_metrics:
                # Initialize variables for tracking best host
                best_host = None
                lowest_host_utilization = float('inf')  # Initialize to positive infinity

                # Iterate over VMs to find the next VM to migrate
                for vm, (vm_cpu_usage, vm_memory_usage) in vm_metrics.items():
                    print(f"\nWhat-if analysis for VM: {vm}")
                    # Calculate the best host for the current VM
                    for host, (cpu_capacity, memory_capacity) in host_metrics.items():
                        new_host_cpu_utilization = cpu_capacity + vm_cpu_usage
                        new_host_memory_utilization = memory_capacity + vm_memory_usage
                        host_utilization = max(new_host_cpu_utilization / cpu_capacity, new_host_memory_utilization / memory_capacity)

                        if host_utilization < lowest_host_utilization and host != vm_metrics[vm]:  # Skip if the VM is already on the best host
                            best_host = host
                            lowest_host_utilization = host_utilization

                    if best_host:
                        # Migrate VM to the best host
                        migrate_vm_to_host(vm, best_host, vcenter_connector.service_instance)
                        print(f"Migrated VM {vm} to host {best_host}. Waiting 1 minute before recalculating host balance.")
                        time.sleep(60)  # Wait for 1 minute before recalculating host balance

                        # Remove the migrated VM from the metrics
                        del vm_metrics[vm]

                        # Update host metrics after migration
                        host_metrics = get_host_metrics(vcenter_connector.service_instance)
                        if not host_metrics:
                            print("Failed to retrieve host metrics after migration. Exiting.")
                            return
                        break  # Move to the next VM after migration
                    else:
                        print(f"No suitable host found for VM {vm}")

        finally:
            vcenter_connector.disconnect()

if __name__ == "__main__":
    main()
