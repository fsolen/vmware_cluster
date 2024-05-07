from pyVim import connect
from pyVmomi import vim

def connect_to_vcenter(host, username, password):
    try:
        vcenter = connect.SmartConnectNoSSL(host=host, user=username, pwd=password)
        return vcenter
    except Exception as e:
        print(f"Error connecting to vCenter Server: {str(e)}")
        return None

def disconnect_from_vcenter(vcenter):
    try:
        if vcenter:
            connect.Disconnect(vcenter)
            print("Disconnected from vCenter Server.")
    except Exception as e:
        print(f"Error disconnecting from vCenter Server: {str(e)}")

def get_vm_with_tag_category(vcenter, category_name):
    vms_by_tag_combinations = {}
    
    try:
        content = vcenter.RetrieveContent()
        tag_manager = content.tagManager
        category = tag_manager.FindCategory(category_name)
        
        if category:
            tag_id = category.id
            vm_tags = tag_manager.ListTagsAttachedToCategory(tag_id)
            
            for tag in vm_tags:
                for vm in tag.vm:
                    tag_combination = tuple(sorted((tag_key.key, tag.val) for tag_key in tag_key))
                    if tag_combination not in vms_by_tag_combinations:
                        vms_by_tag_combinations[tag_combination] = []
                    vms_by_tag_combinations[tag_combination].append(vm)
        
        return vms_by_tag_combinations
    except Exception as e:
        print(f"Error retrieving VMs: {str(e)}")
        return {}

def distribute_vms_evenly(vcenter, category_name):
    try:
        # Get all available hosts
        content = vcenter.RetrieveContent()
        container_view = content.viewManager.CreateContainerView(content.rootFolder,
                                                                 [vim.HostSystem],
                                                                 True)
        all_hosts = container_view.view
        
        # Get VMs with tags from the specified tag category
        vms_by_tag_combinations = get_vm_with_tag_category(vcenter, category_name)
        
        if vms_by_tag_combinations:
            num_hosts = len(all_hosts)
            
            # Distribute VMs evenly across hosts for each tag combination
            for vms in vms_by_tag_combinations.values():
                num_vms = len(vms)
                vms_per_host = num_vms // num_hosts
                remaining_vms = num_vms % num_hosts
                
                host_index = 0
                for i, vm in enumerate(vms):
                    host = all_hosts[host_index]
                    migrate_vm(vm, host)
                    
                    if (i + 1) % vms_per_host == 0:
                        host_index += 1
                        if host_index >= num_hosts:
                            host_index = num_hosts - 1
                            
                        if remaining_vms > 0:
                            remaining_vms -= 1
                            host_index += 1
            
            print("VMs distributed evenly across hosts.")
    except Exception as e:
        print(f"Error distributing VMs: {str(e)}")

def migrate_vm(vm, destination_host):
    try:
        resource_pool = destination_host.parent.resourcePool
        migration_priority = vim.VirtualMachine.MovePriority.defaultPriority
        task = vm.Relocate(resource_pool, migration_priority, host=destination_host)
        print(f"Migrating VM {vm.name} to {destination_host.name}")
        return task
    except Exception as e:
        print(f"Error migrating VM: {str(e)}")

def main():
    # Connect to vCenter Server
    vcenter = connect_to_vcenter("vcenter.fatihsolen.com", "fatihsolen", "*******")
    
    if vcenter:
        try:
            # Specify the tag category name
            category_name = "hostgroup"
            
            # Distribute VMs with the specified tag category evenly across hosts
            distribute_vms_evenly(vcenter, category_name)
            
        finally:
            # Disconnect from vCenter Server
            disconnect_from_vcenter(vcenter)

if __name__ == "__main__":
    main()
