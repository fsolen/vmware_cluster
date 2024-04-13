from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import atexit
import ssl
import datetime

# Connect to vCenter
def connect_to_vcenter(host, username, password):
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    context.verify_mode = ssl.CERT_NONE
    service_instance = SmartConnect(host=host, user=username, pwd=password, sslContext=context)
    atexit.register(Disconnect, service_instance)
    return service_instance

# Calculate datastore usage for a specific VM
def calculate_vm_datastore_usage(vm):
    datastore_usage = {}
    for disk in vm.config.hardware.device:
        if isinstance(disk, vim.vm.device.VirtualDisk):
            datastore_name = disk.backing.datastore.info.name
            if datastore_name not in datastore_usage:
                datastore_usage[datastore_name] = 0
            datastore_usage[datastore_name] += disk.capacityInBytes
    return datastore_usage

# Calculate datastore usage for all VMs in the cluster
def calculate_cluster_datastore_usage(cluster):
    datastore_usage = {}
    for host in cluster.host:
        for vm in host.vm:
            vm_datastore_usage = calculate_vm_datastore_usage(vm)
            for datastore, usage in vm_datastore_usage.items():
                if datastore not in datastore_usage:
                    datastore_usage[datastore] = 0
                datastore_usage[datastore] += usage
    return datastore_usage

# Find the optimal datastore based on available space
def find_optimal_datastore(cluster):
    optimal_datastore = None
    max_free_space = 0

    for datastore in cluster.datastore:
        free_space = datastore.summary.freeSpace
        if free_space > max_free_space:
            optimal_datastore = datastore
            max_free_space = free_space

    return optimal_datastore

# Perform storage vMotion
def perform_storage_vmotion(vm, target_datastore):
    start_time = datetime.datetime.now()
    task = vm.RelocateVM_Task(datastore=target_datastore)
    task_result = task.wait_for_completion()
    end_time = datetime.datetime.now()
    migration_duration = end_time - start_time
    migration_info = {
        "DateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "VMName": vm.name,
        "OldDatastore": vm.storage.perDatastoreUsage[0].datastore.info.name,
        "NewDatastore": target_datastore.name,
        "Duration": str(migration_duration)
    }
    log_migration_details(migration_info)

# Log migration details
def log_migration_details(migration_info):
    log_file = "/var/log/dbalancer.log"
    with open(log_file, "a") as f:
        f.write(f"DateTime: {migration_info['DateTime']}, VMName: {migration_info['VMName']}, OldDatastore: {migration_info['OldDatastore']}, NewDatastore: {migration_info['NewDatastore']}, Duration: {migration_info['Duration']}\n")

# Log datastore usage
def log_datastore_usage(datastore_usage):
    log_file = "/var/log/dbalancer.log"
    with open(log_file, "a") as f:
        f.write("Datastore Usage:\n")
        for datastore, usage in datastore_usage.items():
            f.write(f"Datastore: {datastore}, Usage: {usage} bytes\n")

# Main function
def main():
    service_instance = connect_to_vcenter("vcenter.fatihsolen.com", "fatihsolen", "*******")
    content = service_instance.RetrieveContent()
    datacenter = content.rootFolder.childEntity[0]
    cluster_name = "ist"

    cluster = None
    for child in datacenter.hostFolder.childEntity:
        if isinstance(child, vim.ClusterComputeResource) and child.name == cluster_name:
            cluster = child
            break

    if not cluster:
        print(f"Cluster '{cluster_name}' not found.")
        return

    datastore_usage = calculate_cluster_datastore_usage(cluster)
    log_datastore_usage(datastore_usage)

    all_vms = [vm for host in cluster.host for vm in host.vm]

    optimal_datastore = find_optimal_datastore(cluster)

    # Log cluster name
    with open("/var/log/dbalancer.log", "a") as f:
        f.write(f"Cluster Name: {cluster_name}\n")

    # Log summary information for all datastores
    with open("/var/log/dbalancer.log", "a") as f:
        f.write("Datastore Summary Information:\n")
        for datastore in cluster.datastore:
            f.write(f"Datastore: {datastore.name}, Capacity: {datastore.summary.capacity} bytes, FreeSpace: {datastore.summary.freeSpace} bytes\n")

    for vm in all_vms:
        perform_storage_vmotion(vm, optimal_datastore)

        migration_info = {
            "DateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "VMName": vm.name,
            "OldDatastore": vm.storage.perDatastoreUsage[0].datastore.info.name,
            "NewDatastore": optimal_datastore.name,
            "Duration": str(datetime.datetime.now() - migration_start_time)
        }
        log_migration_details(migration_info)

if __name__ == "__main__":
    main()
