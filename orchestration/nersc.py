from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT
import time, json, os, logging
from orchestration.ptycho_jobscript import (
    get_job_script,
    cdtool_args_string,
    ptychocam_args_string,
    cdtools_parms,
    ptychocam_parms,
)


class NerscClient:
    def __init__(
        self,
        path_client_id,
        path_priv_key,
        logger=None,
    ):
        self.path_client_id = path_client_id
        self.path_private_key = path_priv_key

        self.logger = (
            logging.basicConfig(level=logging.INFO) if logger is None else logger
        )

        self.client_id = None
        self.pri_key = None
        self.session = None
        self.init_session()
        self.task = None
        self.task_id = None
        self.job = None
        self.jobid = None
        self.job_state = None
        self.job_script_string = None
        self.has_ran = False

    def get_client_id(self):
        with open(self.path_client_id, "r") as f:
            self.client_id = f.read()

    def get_private_key(self):
        with open(self.path_private_key, "r") as f:
            self.private_key = f.read()

    def init_session(
        self,
        token_url="https://oidc.nersc.gov/c2id/token",
        grant_type="client_credentials",
    ):
        self.get_client_id()
        self.get_private_key()
        self.session = OAuth2Session(
            self.client_id,
            self.private_key,
            PrivateKeyJWT(token_url),
            grant_type=grant_type,
            token_endpoint=token_url,
        )
        self.session.fetch_token()

    def request_task_status(self):
        r = self.session.get(
            "https://api.nersc.gov/api/v1.2/tasks/{}".format(self.task_id)
        )
        self.task = r.json()

    def request_job_status(self):
        r = self.session.get(
            "https://api.nersc.gov/api/v1.2/compute/jobs/perlmutter/{}".format(
                self.jobid
            )
        )
        self.job = r.json()

    def update_task_id(self):
        self.task_id = self.task["task_id"]

    def update_job_id(self):
        task_result = self.task["result"]
        if task_result is None:
            self.logger.info(f"Waiting for job submission with task_id {self.task_id}")
        elif isinstance(task_result, str):
            r = json.loads(task_result)
            self.jobid = r["jobid"]

    def update_job_state(self):
        self.request_job_status()
        job_output = self.job["output"]
        if len(job_output) > 0:
            self.job_state = job_output[0]["state"]
            if all([not self.has_ran, self.job_state == "RUNNING"]):
                self.has_ran = True
        elif self.has_ran & (len(job_output) == 0):
            self.job_state = "COMPLETE"
            self.logger.info(f"Job {self.jobid} with COMPLETE status")

    def submit_job(self, script_string):
        self.task = None
        self.job = None
        self.jobid = None
        self.task_id = None
        self.has_ran = False

        job_api_url = "https://api.nersc.gov/api/v1.2/compute/jobs/perlmutter"
        data = {"job": script_string, "isPath": False}
        r = self.session.post(job_api_url, data=data)
        self.task = r.json()
        self.update_task_id()
        self.logger.info(f"Submitted task id: {self.task_id}")

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

    def task_wait(self, max_wait_seconds=600, sleep_time=2):
        start = time.time()
        self.job_state = "QUEUE"
        done = False
        self.update_task_id()

        while not done:
            self.request_task_status()
            self.logger.info(f"Task status: {self.task}")
            elapsed = time.time() - start

            if elapsed > max_wait_seconds:
                self.logger.info(f"Waiting for completion of task but time out ")
                self.logger.info(
                    f"Configured to wait {max_wait_seconds}, elapsed is {elapsed} "
                )
                self.logger.info(f"Job may complete in background.")

            if "id" not in list(self.task.keys()):
                self.logger.info(
                    f"Task {self.task_id} not found and may have been completed"
                )
                done = True

            elif (self.task["status"] == "completed") | (self.task["status"] == "new"):
                if self.jobid is None:
                    self.update_job_id()
                else:
                    self.update_job_state()

                if self.job_state == "COMPLETE":
                    done = True
                    self.logger.info(f"Job {self.jobid} has been completed")
                self.logger.info(f"Job{self.jobid} with {self.job_state} status")

            time.sleep(sleep_time)

        return True
