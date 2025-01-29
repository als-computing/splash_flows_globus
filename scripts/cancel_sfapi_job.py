from dotenv import load_dotenv
import json
import logging
import os

from authlib.jose import JsonWebKey
from sfapi_client import Client
from sfapi_client.compute import Machine


load_dotenv()
logger = logging.getLogger(__name__)

client_id_path = os.getenv("PATH_NERSC_CLIENT_ID")
client_secret_path = os.getenv("PATH_NERSC_PRI_KEY")

if not client_id_path or not client_secret_path:
    logger.error("NERSC credentials paths are missing.")
    raise ValueError("Missing NERSC credentials paths.")
if not os.path.isfile(client_id_path) or not os.path.isfile(client_secret_path):
    logger.error("NERSC credential files are missing.")
    raise FileNotFoundError("NERSC credential files are missing.")

client_id = None
client_secret = None
with open(client_id_path, "r") as f:
    client_id = f.read()

with open(client_secret_path, "r") as f:
    client_secret = JsonWebKey.import_key(json.loads(f.read()))

with Client(client_id, client_secret) as client:
    perlmutter = client.compute(Machine.perlmutter)
    # job = perlmutter.submit_job(job_path)
    jobs = perlmutter.jobs(user="dabramov")
    for job in jobs:
        logger.info(f"Cancelling job: {job.jobid}")
        job.cancel()
