from pyVim import connect
from pyVmomi import vim
import ssl
import logging

logger = logging.getLogger('fdrs')

class ConnectionManager:
    """
    Handles connection and disconnection to vCenter Server
    """

    def __init__(self, vcenter_ip, username, password):
        self.vcenter_ip = vcenter_ip
        self.username = username
        self.password = password
        self.service_instance = None

    def connect(self):
        """
        Establishes a secure connection to vCenter
        """
        try:
            logger.info(f"Connecting to vCenter {self.vcenter_ip}...")

            # Create an unverified SSL context (ignore SSL certs)
            context = ssl._create_unverified_context()

            self.service_instance = connect.SmartConnect(
                host=self.vcenter_ip,
                user=self.username,
                pwd=self.password,
                port=443,
                sslContext=context
            )

            if not self.service_instance:
                logger.error("Failed to connect to vCenter!")
                raise Exception("Service instance is None")

            logger.info("Successfully connected to vCenter!")
            return self.service_instance

        except Exception as e:
            logger.error(f"vCenter connection error: {e}")
            raise

    def disconnect(self):
        """
        Disconnects from vCenter
        """
        try:
            if self.service_instance:
                connect.Disconnect(self.service_instance)
                logger.info("Disconnected from vCenter cleanly.")
        except Exception as e:
            logger.error(f"Error during disconnection: {e}")

