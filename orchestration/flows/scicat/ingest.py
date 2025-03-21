import importlib
import os
from typing import List

from pyscicat.client import ScicatClient, from_credentials
from prefect import flow, task, get_run_logger

from orchestration.flows.scicat.utils import Issue


@flow(name="scicat_dataset_ingest")
def ingest_dataset(file_path: str, ingestor: str):
    """ Ingest a file into SciCat.

    Parameters
    ----------
    file_path : str
        Path where the file can be found on whatever server is processing this task
    ingestor_module : str
        Thy python module that contains the ingest function, e.g. "foo.bar.ingestor"
    """
    ingest_dataset_task(file_path, ingestor)


@task(name="ingest_scicat")
def ingest_dataset_task(file_path: str, ingestor_module: str):
    """ Ingest a file into SciCat.

    Parameters
    ----------
    file_path : str
        Path where the file can be found on whatever server is processing this task
    ingestor_module : str
        Thy python module that contains the ingest function, e.g. "foo.bar.ingestor"
    """
    logger = get_run_logger()
    import dotenv
    dotenv.load_dotenv()
    SCICAT_API_URL = os.getenv("SCICAT_API_URL")
    SCICAT_INGEST_USER = os.getenv("SCICAT_INGEST_USER")
    SCICAT_INGEST_PASSWORD = os.getenv("SCICAT_INGEST_PASSWORD")

    # files come in with the full pasth on the server that they
    # were loaded from.

    # relative path: raw/...
    # ingestor api maps /globa/cfs/cdirs/als/data_mover to /data_mover
    # so we want to prepend /data_mover/8.3.2
    # if relative_path[0] == "/":
    #     relative_path = relative_path[1:]
    # ingest_path = os.path.join("/data_mover/8.3.2", file_path)
    logger.info(
        f"Sending ingest job to {SCICAT_API_URL} for file {file_path}"
    )
    try:
        scicat_client = from_credentials(
            SCICAT_API_URL,
            SCICAT_INGEST_USER,
            SCICAT_INGEST_PASSWORD)
    except Exception as e:
        logger.warning(f"Failed to create SciCat client using pyscicat method: {e}")

    try:
        import requests
        from urllib.parse import urljoin

        url = urljoin(SCICAT_API_URL, "auth/login")
        logger.info(url)
        response = requests.post(
            url=url,
            json={"username": SCICAT_INGEST_USER, "password": SCICAT_INGEST_PASSWORD},
            stream=False,
            verify=True,
        )
        logger.info(f"Login response: {response.json()}")
        scicat_client = ScicatClient(SCICAT_API_URL, response.json()["access_token"])
        logger.info("Logged in to SciCat.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to log in to SciCat: {e}")
        raise e
    except Exception as e:
        logger.error(f"Failed to log in to SciCat: {e}")
        raise e

    ingestor_module = importlib.import_module(ingestor_module)
    issues: List[Issue] = []
    new_dataset_id = ingestor_module.ingest(
        scicat_client,
        file_path,
        issues,
    )
    if len(issues) > 0:
        logger.error(f"SciCat ingest failed with {len(issues)} issues")
        for issue in issues:
            logger.error(issue)
        raise Exception("SciCat ingest failed")
    return new_dataset_id


if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv
    load_dotenv()
    ingest_dataset(sys.argv[1], sys.argv[2])
