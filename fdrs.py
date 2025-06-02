#!/usr/bin/env python3

import argparse
import logging
import sys
from modules.banner import print_banner
from modules.connection_manager import ConnectionManager
from modules.resource_monitor import ResourceMonitor
from modules.constraint_manager import ConstraintManager
from modules.cluster_state import ClusterState
from modules.load_evaluator import LoadEvaluator
from modules.migration_planner import MigrationManager
from modules.scheduler import Scheduler
# Removed: from modules.logger import Logger
import logging # Added for standard Python logging
import sys # Added for sys.stdout

# logger = Logger() # Removed custom logger instantiation
# Initialize logger at module level, will be configured in main()
logger = logging.getLogger('fdrs') # Use 'fdrs' as the main logger name

def parse_args():
    """
    Parse the command-line arguments.
    """
    parser = argparse.ArgumentParser(description="FDRS - Fully Distributed Resource Scheduler")
    parser.add_argument("--vcenter", required=True, help="vCenter hostname or IP address")
    parser.add_argument("--username", required=True, help="vCenter username")
    parser.add_argument("--password", required=True, help="vCenter password")
    parser.add_argument("--dry-run", action="store_true", help="Enable dry-run mode")
    parser.add_argument("--aggressiveness", type=int, default=3, choices=range(1, 6), help="Aggressiveness level (1-5)")
    parser.add_argument("--balance", action="store_true", help="Auto-balance the cluster based on selected metrics")
    parser.add_argument("--metrics", type=str, default="cpu,memory,disk,network", help="Comma-separated list of metrics to balance: cpu,memory,disk,network")
    parser.add_argument("--apply-anti-affinity", action="store_true", help="Apply anti-affinity rules only")

    return parser.parse_args()

def main():
    print_banner()

    args = parse_args()

    # Configure standard Python logging
    logging.basicConfig(
        level=logging.DEBUG, # Set to DEBUG to capture all levels from modules
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)] # Ensure output to console
    )
    logging.getLogger('fdrs').setLevel(logging.INFO)
    # The module-level logger 'logger' will now use this basicConfig.

    logger.info("Starting FDRS...")
    connection_manager = ConnectionManager(args.vcenter, args.username, args.password)
    service_instance = connection_manager.connect()

    resource_monitor = ResourceMonitor(service_instance)
    cluster_state = ClusterState(service_instance)
    cluster_state.update_metrics(resource_monitor) # Ensure metrics are populated
    state = cluster_state.get_cluster_state()

    if args.apply_anti_affinity:
        logger.info("Applying anti-affinity rules only...")
        constraint_manager = ConstraintManager(cluster_state)
        constraint_manager.apply()
        # Plan and execute migrations for anti-affinity violations
        # Instantiate LoadEvaluator even for anti-affinity for consistent MigrationManager instantiation
        load_evaluator = LoadEvaluator(state['hosts']) 
        migration_planner = MigrationManager(cluster_state, constraint_manager, load_evaluator, aggressiveness=args.aggressiveness)
        migrations = migration_planner.plan_migrations()
        if migrations:
            scheduler = Scheduler(connection_manager, dry_run=args.dry_run)
            scheduler.execute_migrations(migrations)
        else:
            logger.info("No anti-affinity migrations needed.")
        connection_manager.disconnect()
        return

    if args.balance:
        logger.info(f"Auto-balancing cluster using metrics: {args.metrics}")
        metrics_list = [m.strip() for m in args.metrics.split(",") if m.strip()] # Renamed 'metrics' to 'metrics_list'
        
        load_evaluator = LoadEvaluator(state['hosts'])
        constraint_manager = ConstraintManager(cluster_state)
        # Consider applying constraints only if migrations are attempted or part of planner
        # constraint_manager.apply() # This might be better inside MigrationManager or just before planning specific moves

        migration_planner = MigrationManager(cluster_state, constraint_manager, load_evaluator, aggressiveness=args.aggressiveness)

        # Log statistical imbalance for informational purposes
        statistical_imbalance_detected = load_evaluator.evaluate_imbalance(metrics_to_check=metrics_list, aggressiveness=args.aggressiveness)
        if statistical_imbalance_detected:
            logger.info("Statistical load imbalance detected by LoadEvaluator. MigrationPlanner will now determine actions.")
        else:
            logger.info("LoadEvaluator reports no significant statistical imbalance. MigrationPlanner will still check for individual host overloads and anti-affinity rules.")

        # Always call plan_migrations if in balancing mode.
        # MigrationManager's plan_migrations should handle constraint_manager.apply() internally if it's specific to its plans.
        # For now, assume constraint_manager.apply() is okay here or handled by MigrationManager.
        # If constraint_manager.apply() modifies state that MigrationManager reads, its placement matters.
        # Let's ensure constraints are applied before planning.
        logger.info("Applying constraints before migration planning...")
        constraint_manager.apply()
        
        logger.info("Proceeding with migration planning phase...")
        migrations = migration_planner.plan_migrations() 

        if migrations:
            logger.info(f"Found {len(migrations)} migration(s) to perform for load balancing and/or anti-affinity.")
            scheduler = Scheduler(connection_manager, dry_run=args.dry_run)
            scheduler.execute_migrations(migrations) # Ensure this method exists and is correct
        else:
            logger.info("Migration planning complete. No actionable migrations found or needed at this time.")
        
        connection_manager.disconnect()
        return

    logger.info("Running default FDRS workflow (evaluating load and planning migrations if needed)...")
    load_evaluator = LoadEvaluator(state['hosts'])
    constraint_manager = ConstraintManager(cluster_state)
    migration_planner = MigrationManager(cluster_state, constraint_manager, load_evaluator, aggressiveness=args.aggressiveness)

    # Log statistical imbalance for informational purposes
    statistical_imbalance_detected = load_evaluator.evaluate_imbalance(aggressiveness=args.aggressiveness)
    if statistical_imbalance_detected:
        logger.info("Statistical load imbalance detected by LoadEvaluator. MigrationPlanner will now determine actions.")
    else:
        logger.info("LoadEvaluator reports no significant statistical imbalance. MigrationPlanner will still check for individual host overloads and anti-affinity rules.")
    
    logger.info("Applying constraints before migration planning...")
    constraint_manager.apply()

    logger.info("Proceeding with migration planning phase...")
    migrations = migration_planner.plan_migrations()

    if migrations:
        logger.info(f"Found {len(migrations)} migration(s) to perform for load balancing and/or anti-affinity.")
        scheduler = Scheduler(connection_manager, dry_run=args.dry_run)
        scheduler.execute_migrations(migrations)
    else:
        logger.info("Migration planning complete. No actionable migrations found or needed at this time.")

    # Disconnect from vCenter
    connection_manager.disconnect()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("An error occurred: {}".format(e))
        sys.exit(1)
