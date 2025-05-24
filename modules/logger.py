import os
import datetime
import json
from typing import Dict, List, Optional, Any

class Logger:
    COLORS = {
        "INFO": "\033[94m",
        "SUCCESS": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "EVENT": "\033[95m",  # Purple for events
        "ENDC": "\033[0m",
    }

    def __init__(self, log_dir="/var/log/fdrs", log_file="fdrs.log"):
        self.log_path = os.path.join(log_dir, log_file)
        self.events_path = os.path.join(log_dir, "events.json")
        os.makedirs(log_dir, exist_ok=True)
        self.events_history: Dict[str, List[Dict[str, Any]]] = {
            "migrations": [],
            "resource_changes": [],
            "errors": []
        }
        self._load_events()

    def log(self, level, message):
        color = self.COLORS.get(level, "")
        endc = self.COLORS["ENDC"]
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{now}] [{level}] {message}"

        # Console with color
        print(f"{color}{formatted_message}{endc}")

        # Append to file
        with open(self.log_path, "a") as f:
            f.write(formatted_message + "\n")

    def info(self, message):
        self.log("INFO", message)

    def success(self, message):
        self.log("SUCCESS", message)

    def warning(self, message):
        self.log("WARNING", message)

    def error(self, message):
        self.log("ERROR", message)

    def _load_events(self):
        """Load events history from file if it exists"""
        if os.path.exists(self.events_path):
            try:
                with open(self.events_path, 'r') as f:
                    self.events_history = json.load(f)
            except json.JSONDecodeError:
                self.warning(f"Could not load events from {self.events_path}. Starting with empty history.")

    def _save_events(self):
        """Save events history to file"""
        try:
            with open(self.events_path, 'w') as f:
                json.dump(self.events_history, f, indent=2)
        except Exception as e:
            self.error(f"Failed to save events history: {e}")

    def track_event(self, event_type: str, event_data: Dict[str, Any]):
        """
        Track an event with timestamp and data.
        event_type can be 'migration', 'resource_change', or 'error'
        """
        timestamp = datetime.datetime.now().isoformat()
        event = {
            'timestamp': timestamp,
            **event_data
        }
        
        if event_type not in self.events_history:
            self.events_history[event_type] = []
            
        self.events_history[event_type].append(event)
        self._save_events()
        
        # Log the event
        event_msg = f"Event [{event_type}]: " + " | ".join(f"{k}={v}" for k, v in event_data.items())
        self.log("EVENT", event_msg)

    def track_migration(self, vm_name: str, source_host: str, target_host: str, reason: str, metrics: Optional[Dict] = None):
        """Track a VM migration event"""
        self.track_event('migration', {
            'vm_name': vm_name,
            'source_host': source_host,
            'target_host': target_host,
            'reason': reason,
            'metrics': metrics or {}
        })

    def track_resource_change(self, resource_type: str, entity_name: str, old_value: Any, new_value: Any, reason: str):
        """Track a significant resource usage change"""
        self.track_event('resource_change', {
            'resource_type': resource_type,
            'entity_name': entity_name,
            'old_value': old_value,
            'new_value': new_value,
            'reason': reason
        })

    def get_events(self, event_type: Optional[str] = None, start_time: Optional[str] = None, 
                  end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get events filtered by type and time range.
        Returns all events if no filters provided.
        """
        events = []
        for etype, elist in self.events_history.items():
            if event_type and etype != event_type:
                continue
                
            for event in elist:
                if start_time and event['timestamp'] < start_time:
                    continue
                if end_time and event['timestamp'] > end_time:
                    continue
                events.append(event)
                
        return sorted(events, key=lambda x: x['timestamp'])
