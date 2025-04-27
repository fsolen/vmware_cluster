import os
import datetime

class Logger:
    COLORS = {
        "INFO": "\033[94m",
        "SUCCESS": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "ENDC": "\033[0m",
    }

    def __init__(self, log_dir="/var/log/fdrs", log_file="fdrs.log"):
        self.log_path = os.path.join(log_dir, log_file)
        os.makedirs(log_dir, exist_ok=True)

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
