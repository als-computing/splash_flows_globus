import json
import logging
import time

from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT

from orchestration.ptycho_jobscript import (
    get_job_script,
    cdtool_args_string,
    ptychocam_args_string,
    cdtools_parms,
    ptychocam_parms,
)

from orchestration.nersc import NerscClient

class NerscPtychoClient(NerscClient):
    def __init__(
        self,
        path_client_id,
        path_priv_key,
        logger=None,
    ):
        super().__init__(path_client_id, path_priv_key, logger)

    def cdtools(self, cxiname, path_job_script, path_cdtools_nersc, n_gpu, **kwargs):
        args_string = cdtool_args_string(
            cxiname, path_cdtools_nersc, cdtools_parms, **kwargs
        )
        job_script = get_job_script(path_job_script, n_gpu, args_string)
        self.logger.info(f"Job script: {job_script}")

        self.submit_job(job_script)
        self.task_wait()

    def ptychocam(
        self, cxiname, path_job_script, path_ptychocam_nersc, n_gpu, **kwargs
    ):
        args_string = ptychocam_args_string(
            cxiname, path_ptychocam_nersc, ptychocam_parms, **kwargs
        )
        job_script = get_job_script(path_job_script, n_gpu, args_string)

        self.logger.info(f"Job script: {job_script}")

        self.submit_job(job_script)
        self.task_wait()
