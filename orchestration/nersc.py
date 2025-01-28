'''
DEPRECATION WARNING: NerscClient is deprecated and will be removed when we refactor the ptychography code
'''
import functools
import json
import logging
# from pathlib import Path
# import time
import warnings

# from authlib.integrations.requests_client import OAuth2Session
# from authlib.oauth2.rfc7523 import PrivateKeyJWT
from authlib.jose import JsonWebKey

from sfapi_client import Client
# from sfapi_client._sync.client import SFAPI_BASE_URL, SFAPI_TOKEN_URL
from sfapi_client.compute import Machine

# Temporary patch till the sfapi_client is updated
from sfapi_client.jobs import JobSacct
# from sfapi_client.compute import Compute
JobSacct.model_rebuild()


def deprecated_method(message: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.warn(
                message="NerscClient() is deprecated and will be removed in a future version. Use the official NERSC "
                "sfapi_client module instead: https://nersc.github.io/sfapi_client/",
                category=DeprecationWarning,
                stacklevel=2
            )
            return func(*args, **kwargs)
        return wrapper
    return decorator


class NerscClient(Client):
    '''
    DEPRECATION WARNING: NerscClient is deprecated and will be removed when we refactor the ptychography code
    '''
    def __init__(
        self,
        path_client_id,
        path_priv_key,
        logger=None,
    ):
        warnings.warn(
            "NerscClient() is deprecated and will be removed in a future version. "
            "Use the official NERSC sfapi_client module instead: https://nersc.github.io/sfapi_client/",
            DeprecationWarning,
            stacklevel=2,  # Shows warning at caller level
        )

        self.path_client_id = path_client_id
        self.path_private_key = path_priv_key

        self.logger = (
            logging.basicConfig(level=logging.INFO) if logger is None else logger
        )

        # Reading the client_id and private key from the files
        self.client_id = None
        self.pri_key = None
        # self.session = None
        self.init_client_info()

        super().__init__(self.client_id, self.pri_key)

        # NERSC specific directory paths initialization
        self.home_path = None
        self.scratch_path = None
        self.init_directory_paths()

        self.task = None
        self.task_id = None
        self.job = None
        self.jobid = None
        self.job_state = None
        self.job_script_string = None
        self.has_ran = False
        self.perlmutter = self.compute(Machine.perlmutter)

    @deprecated_method()
    def get_client_id(self):
        with open(self.path_client_id, "r") as f:
            self.client_id = f.read()

    @deprecated_method()
    def get_private_key(self):
        with open(self.path_private_key, "r") as f:
            self.pri_key = JsonWebKey.import_key(json.loads(f.read()))

    @deprecated_method()
    def get_machine_status(self):
        return self.perlmutter.status

    @deprecated_method()
    def init_client_info(
        self
    ):
        self.get_client_id()
        self.get_private_key()

    @deprecated_method()
    def init_directory_paths(self):
        self.home_path = f"/global/homes/{self.user().name[0]}/{self.user().name}"
        self.scratch_path = f"/pscratch/sd/{self.user().name[0]}/{self.user().name}"

    @deprecated_method()
    def request_job_status(self):
        self.job = self.perlmutter.job(jobid=self.jobid)

    @deprecated_method()
    def update_job_id(self):
        if self.job is None:
            self.logger.info("No job found")
        else:
            self.jobid = self.job.jobid

    @deprecated_method()
    def update_job_state(self):
        self.request_job_status()
        self.job_state = self.job.state

        if self.job_state == "RUNNING":
            self.has_ran = True
        elif self.job_state == "COMPLETE":
            self.logger.info(f"Job {self.jobid} with COMPLETE status")

    @deprecated_method()
    def submit_job(self, job_script):
        self.task = None
        self.job = None
        self.jobid = None
        self.task_id = None
        self.has_ran = False

        self.job_script_string = job_script
        self.logger.info(f"Submitting job with script: {job_script}")
        self.job = self.perlmutter.submit_job(job_script)
        self.update_job_id()
        # self.update_job_state()
        self.logger.info(f"Submitted job id: {self.jobid}")
