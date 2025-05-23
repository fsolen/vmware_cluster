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
from modules.logger import Logger

logger = Logger()

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
    # Print the banner
    print_banner()

    args = parse_args()

    logger.info("Starting FDRS...")
    connection_manager = ConnectionManager(args.vcenter, args.username, args.password)
    service_instance = connection_manager.connect()

    resource_monitor = ResourceMonitor(service_instance)
    cluster_state = ClusterState(service_instance)
    state = cluster_state.get_cluster_state()

    if args.apply_anti_affinity:
        logger.info("Applying anti-affinity rules only...")
        constraint_manager = ConstraintManager(service_instance)
        constraint_manager.apply()
        connection_manager.disconnect()
        return

    if args.balance:
        logger.info(f"Auto-balancing cluster using metrics: {args.metrics}")
        metrics = [m.strip() for m in args.metrics.split(",") if m.strip()]
        load_evaluator = LoadEvaluator(state)
        imbalance = load_evaluator.evaluate_imbalance(metrics=metrics, aggressiveness=args.aggressiveness)
        if imbalance:
            logger.info("Load imbalance detected. Planning migrations...")
            constraint_manager = ConstraintManager(service_instance)
            constraint_manager.apply()
            migration_planner = MigrationManager(service_instance, state)
            migration_planner.plan_migrations()
            scheduler = Scheduler(service_instance, migration_planner, dry_run=args.dry_run)
            scheduler.execute_migrations()
        else:
            logger.info("No imbalance detected. No migrations needed.")
        connection_manager.disconnect()
        return

    # Default: run full workflow as before
    # Connect to vCenter
    logger.info("Starting FDRS...")
    connection_manager = ConnectionManager(args.vcenter, args.username, args.password)
    service_instance = connection_manager.connect()

    # Monitor Resources (this can be done in a separate thread)
    resource_monitor = ResourceMonitor(service_instance)
    # resource_monitor.start_monitoring()  # Uncomment to enable live resource monitoring in the background

    # Get the cluster state
    cluster_state = ClusterState(service_instance)
    state = cluster_state.get_cluster_state()

    # Evaluate the load and check for imbalance
    load_evaluator = LoadEvaluator(state)
    imbalance = load_evaluator.evaluate_imbalance(aggressiveness=args.aggressiveness)

    if imbalance:
        logger.info("Load imbalance detected. Planning migrations...")

        # Apply auto Anti-Affinity rules (Distribute VMs)
        constraint_manager = ConstraintManager(service_instance)
        constraint_manager.apply()

        # Plan migrations
        migration_planner = MigrationManager(service_instance, state)
        migration_planner.plan_migrations()

        # Schedule migrations
        scheduler = Scheduler(service_instance, migration_planner, dry_run=args.dry_run)
        scheduler.execute_migrations()
    else:
        logger.info("No imbalance detected. No migrations needed.")

    # Disconnect from vCenter
    connection_manager.disconnect()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("An error occurred: {}".format(e))
        sys.exit(1)
