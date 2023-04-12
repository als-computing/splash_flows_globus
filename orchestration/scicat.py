import httpx
import logging

logger = logging.getLogger("flows.scicat")


class ScicatIngestError(Exception):
    pass


def submit_ingest(
    scicat_api_url: str, file: str, token, mapping_name: str, logger=logger
):
    logger.info(
        f"Submitting scicat ingest {file}, with mapping {mapping_name} to {scicat_api_url}"
    )
    with httpx.Client() as client:
        data = {
            "file_path": file,
            "mapping_name": mapping_name,
            "ingest_types": ["scicat"],
        }
        response = client.post(f"{scicat_api_url}?api_key={token}", json=data)
        if not response.is_success:
            raise ScicatIngestError(f"error starting scicat ingest job {response}")
        return response.json()


def test(scicat_api_url: str, token, logger=logger):
    logger.info(f"Testing scicat at {scicat_api_url}")
    with httpx.Client() as client:
        response = client.get(f"{scicat_api_url}?api_key={token}")
        if not response.is_success:
            raise ScicatIngestError(f"error testing scicat ingest {response}")
        return True
