import ssl
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

def get_host_metrics(vcenter):
    host_metrics = {}
    host_objects = {}
    try:
        content = vcenter.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(content.rootFolder,
                                                                 [vim.HostSystem],
                                                                 True)
        hosts = container_view.view
      
        for host in hosts:
            summary = host.summary
            cpu_usage = summary.quickStats.overallCpuUsage
            cpu_capacity = host.hardware.cpuInfo.numCpuCores * host.hardware.cpuInfo.hz
            cpu_utilization = (cpu_usage / cpu_capacity) * 100
            
            memory_usage = summary.quickStats.overallMemoryUsage
            memory_capacity = host.hardware.memorySize
            memory_utilization = (memory_usage / memory_capacity) * 100
            
            host_metrics[host.name] = (cpu_utilization, memory_utilization)
            host_objects[host.name] = host  # Map host names to HostSystem objects
        
        return host_metrics, host_objects
    except Exception as e:
        print(f"Error retrieving host metrics: {str(e)}")
        return None, None

def migrate_vm(vm, destination_host):
    try:
        resource_pool = destination_host.parent.resourcePool
        migration_priority = vim.VirtualMachine.MovePriority.defaultPriority
        task = vm.Relocate(resource_pool, migration_priority, host=destination_host)
        return task
    except Exception as e:
        print(f"Error migrating VM: {str(e)}")
        return None

def main():
    vcenter_config_file = "vcenter01_config.yaml"
    vcenter_connector = vCenterConnector(vcenter_config_file)
    
    if vcenter_connector.connect():
        try:
            # Get host metrics
            host_metrics, host_objects = get_host_metrics(vcenter_connector.service_instance)
            
            if host_metrics and host_objects:
                # Identify top utilized host
                top_host = max(host_metrics, key=lambda x: sum(host_metrics[x]))
                print(f"Top Utilized Host: {top_host}")

                # Migrate VMs from the top utilized host to other hosts in the cluster
                content = vcenter_connector.service_instance.RetrieveContent()
                container_view = content.viewManager.CreateContainerView(content.rootFolder,
                                                                         [vim.VirtualMachine],
                                                                         True)
                vms = container_view.view
                
                for vm_obj in vms:
                    if isinstance(vm_obj, vim.VirtualMachine):
                        vm = vm_obj
                        if vm.runtime.host.name == top_host:
                            least_utilized_host_name = min(host_metrics, key=lambda x: sum(host_metrics[x]))
                            least_utilized_host = host_objects.get(least_utilized_host_name)
                            print(f"Type of least_utilized_host: {type(least_utilized_host)}, Value: {least_utilized_host}")
                            print(f"Type of top_host: {type(top_host)}, Value: {top_host}")
                            if least_utilized_host and least_utilized_host != top_host:
                                # Migrate VM to the least utilized host
                                task = migrate_vm(vm, least_utilized_host)
                                if task:
                                    print(f"Migrating VM {vm.name} to {least_utilized_host_name}")
                                    # Wait for migration task to complete
                                    while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
                                        pass
                    else:
                        print(f"Encountered unexpected object type: {type(vm_obj)}")
            
        finally:
            # Disconnect from vCenter Server
            vcenter_connector.disconnect()

if __name__ == "__main__":
    main()
