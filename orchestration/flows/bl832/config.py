from globus_sdk import TransferClient

from orchestration import globus


class Config832:
    def __init__(self) -> None:
        config = globus.get_config()
        self.endpoints = globus.build_endpoints(config)
        self.apps = globus.build_apps(config)
        self.tc: TransferClient = globus.init_transfer_client(self.apps['als_transfer'])
        self.spot832 = self.endpoints['spot832']
        self.data832 = self.endpoints['data832']
        self.nersc832 = self.endpoints['nersc832']
        self.scicat = config['scicat']