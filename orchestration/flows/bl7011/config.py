from globus_sdk import TransferClient
from orchestration.globus import transfer


class Config7011:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data7011 = self.endpoints["data7011"]
        self.data7011_raw = self.endpoints["data7011_raw"]
        self.nersc7011_alsdev_raw = self.endpoints["nersc7011_alsdev_raw"]
