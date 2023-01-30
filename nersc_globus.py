import logging

import globus_sdk
import utils

from globus_sdk import TransferClient

logger = logging.getLogger("data_mover")


class NERSCGlobus:
    def __init__(
        self,
        endpoint_name,
        root_path,
        transfer_client: TransferClient,
        nersc_creds_file,
    ):
        self.transfer_client = transfer_client
        self.endpoint_name = endpoint_name
        self.root_path = root_path
        self.nersc_creds_file = nersc_creds_file
        self.id = -1

    def initialize(self):
        """Initializes the endpoint. If activation fails, reads the current pem file
        for new credentials. Hopefully the pem file has been updated by an external process
        and activates
        """
        transfer_client = self.transfer_client
        cori_ep = transfer_client.endpoint_search(self.endpoint_name)[0]
        cori_id = cori_ep["id"]
        self.id = cori_id

        res = transfer_client.endpoint_autoactivate(cori_id)
        if res["code"] == "AutoActivationFailed":
            logger.info(f'Manual Cori:"  {cori_ep["display_name"]} {cori_id}')
            with open(self.nersc_creds_file, "r") as pem_file:
                nersc_pem = pem_file.read()
            requirements_data = transfer_client.endpoint_get_activation_requirements(
                cori_id
            ).data
            proxy_lifetime = 12
            # try with existing
            filled_requirements_data = (
                utils.fill_delegate_proxy_activation_requirements(
                    requirements_data, nersc_pem, lifetime_hours=proxy_lifetime or 12
                )
            )
            res = transfer_client.endpoint_activate(cori_id, filled_requirements_data)
            logger.info(f'Status {res["code"]}')

    def ls(self):
        res = self.transfer_client.operation_ls(self.id)
        return res

    def mkdir(self, path):
        breakpoint()
        res = self.transfer_client.operation_mkdir(self.id, path=path)
        return res
