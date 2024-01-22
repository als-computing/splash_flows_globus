# Note that you must have prefect in your environment. Recommend going up one directory and 
# running `pip install -r requirements.txt`
#
import asyncio
import os
import prefect


def get_prefect_client(httpx_settings=None):
    # This prefect client uses prefect settings to set the the api url and api key. This 
    # can be set in a number of ways. This script ASSUMES that you have set environment variables
    # PREFECT_API_URL = https://servername/api
    # PREFECT_API_KEY = the api key given to you
    # httpx_settings allows you to affect the http client that the prefect client uses
    return prefect.get_client(httpx_settings=httpx_settings)


def get_prefect_client_2(prefect_api_url, prefect_api_key, httpx_settings=None):
    # Same prefect client, but if you know the url and api_key
    # httpx_settings allows you to affect the http client that the prefect client uses
    return prefect.PrefectClient(
        prefect_api_url,
        api_key=prefect_api_key,
        httpx_settings=httpx_settings)


async def prefect_start_flow(prefect_client, deployment_name, file_path):

    deployment = await prefect_client.read_deployment_by_name(deployment_name)
    flow_run = await prefect_client.create_flow_run_from_deployment(
        deployment.id,
        name=os.path.basename(file_path),
        parameters={"file_path": file_path},
    )
    return flow_run


client = get_prefect_client()
asyncio.run(prefect_start_flow(client, "transfer_auto_recon/transfer_auto_recon", "/a/file/path"))
