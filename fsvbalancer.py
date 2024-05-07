from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import atexit
import ssl
import datetime

# Connect to vSphere
def connect_to_vcenter(host, username, password):
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    context.verify_mode = ssl.CERT_NONE
    vcenter = SmartConnect(host=host, user=username, pwd=password, sslContext=context)
    atexit.register(Disconnect, vcenter)
    return vcenter

# Calculate basic resource usages from hypervisor host
def calculate_host_resources(host):
    total_cpu = host.summary.hardware.numCpuThreads / 4  # Considering 1 physical core / 4 virtual CPUs
    total_memory = host.summary.hardware.memorySize / (1024 * 1024)  # Convert to MB
    used_cpu = host.summary.quickStats.overallCpuUsage
    used_memory = host.summary.quickStats.overallMemoryUsage
    available_cpu = total_cpu - used_cpu
    available_memory = total_memory - (used_memory / 1024)  # Convert to MB
    return available_cpu, available_memory

# Calculate VM resources
def calculate_vm_resources(vm):
    cpu = vm.summary.config.numCpu
    memory = vm.summary.config.memorySizeMB
    cpu_usage = vm.summary.quickStats.overallCpuUsage
    return cpu, memory, cpu_usage

# Find the optimal hypervisor host based on available resources
def find_optimal_server(cluster):
    hosts = cluster.host
    optimal_host = None
    max_available_cpu = 0
    max_available_memory = 0

    for host in hosts:
        available_cpu, available_memory = calculate_host_resources(host)
        if available_cpu > max_available_cpu and available_memory > max_available_memory:
            optimal_host = host
            max_available_cpu = available_cpu
            max_available_memory = available_memory

    return optimal_host

# Perform vMotion
def perform_vmotion(vm, target_host):
    start_time = datetime.datetime.now()
    task = vm.RelocateVM_Task(host=target_host)
    task_result = task.wait_for_completion()
    end_time = datetime.datetime.now()
    migration_duration = end_time - start_time
    migration_info = {
        "DateTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "VMName": vm.name,
        "OldHost": vm.runtime.host.name,
        "NewHost": target_host.name,
        "Duration": str(migration_duration)
    }
    log_migration_details(migration_info)

def log_migration_details(migration_info):
    log_file = "/var/log/fsvbalancer.log"
    with open(log_file, "a") as f:
        f.write(f"DateTime: {migration_info['DateTime']}, VMName: {migration_info['VMName']}, OldHost: {migration_info['OldHost']}, NewHost: {migration_info['NewHost']}, Duration: {migration_info['Duration']}\n")

# Main function
def main():
    # Connect to vSphere
    vcenter = connect_to_vcenter("vcenter.fatihsolen.com", "fatihsolen", "*******")

    # Get root folder
    content = vcenter.RetrieveContent()
    datacenter = content.rootFolder.childEntity[0]
    cluster_name = "Istanbul" 

    # Find cluster by name
    cluster = None
    for child in datacenter.hostFolder.childEntity:
        if isinstance(child, vim.ClusterComputeResource) and child.name == cluster_name:
            cluster = child
            break

    if not cluster:
        print(f"Cluster '{cluster_name}' not found.")
        return

    # Get all VMs in the cluster
    all_vms = [vm for host in cluster.host for vm in host.vm]

    # Calculate host resources and print
    for host in cluster.host:
        available_cpu, available_memory = calculate_host_resources(host)
        print(f"Host '{host.name}': Available CPU: {available_cpu} MHz, Available Memory: {available_memory} MB")

    # Calculate VM resources and print
    for vm in all_vms:
        cpu, memory, cpu_usage = calculate_vm_resources(vm)
        print(f"VM '{vm.name}': CPU: {cpu} vCPU(s), Memory: {memory} MB, CPU Usage: {cpu_usage} MHz")

    # Find optimal host
    optimal_host = find_optimal_server(cluster)
    print(f"Optimal host for VM migration: {optimal_host.name}")

    # Perform vMotion for all VMs
    for vm in all_vms:
        perform_vmotion(vm, optimal_host)

if __name__ == "__main__":
    main()
