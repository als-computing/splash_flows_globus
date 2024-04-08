from globus_sdk import TransferClient

from orchestration import globus
from ptycho_nersc import NerscPtychoClient


class Config7012:
    def __init__(
        self,
        path_client_id,
        path_priv_key,
    ) -> None:
        config = globus.get_config()
        self.endpoints = globus.build_endpoints(config)
        self.apps = globus.build_apps(config)
        self.tc: TransferClient = globus.init_transfer_client(self.apps["als_transfer"])
        self.nersc = NerscPtychoClient(
            path_client_id,
            path_priv_key,
        )
        self.nersc7012 = self.endpoints["nersc7012"]
        self.data7012 = self.endpoints["data7012"]
