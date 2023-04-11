import logging

from prefect import flow, get_run_logger
from prefect.blocks.system import JSON

from orchestration.globus import (
    # get_files,
    # get_globus_file_object,
    # is_globus_file_older,
    # prune_files,
    prune_one_safe,
)
from orchestration.flows.bl832.config import Config832


logger = logging.getLogger(__name__)


# @flow
# def prune_one_data832_file(file: str, if_older_than_days: int = 14):
#     logger = get_run_logger()
#     config = Config832()
#     # get data832 object so we can check age
#     data_832_file_obj = get_globus_file_object(config.tc, config.data832, file).result()
#     logger.info(f"file object found on data832")
#     assert data_832_file_obj is not None, "file not found on data832"
#     if not is_globus_file_older(data_832_file_obj, if_older_than_days):
#         logger.info(f"Will not prune, file date {data_832_file_obj['last_modified']} is "
#                     f"newer than {if_older_than_days} days")
#         return

#     # check that file exists at NERSC
#     nersc_832_file_obj = get_globus_file_object(config.tc, config.nersc, file)
#     logger.info(f"file object found on nersc")
#     assert nersc_832_file_obj is not None, "file not found on nersc"

#     prune_files(config.tc, config.data832, [file])


@flow(name="prune_spot832")
def prune_spot832(relative_path: str):
    p_logger = get_run_logger()
    config = Config832()
    globus_settings = JSON.load("globus-settings").value
    max_wait_seconds = globus_settings["max_wait_seconds"]
    p_logger.info(f"Pruning {relative_path} from {config.spot832}")
    prune_one_safe(
        relative_path,
        0,
        config.tc,
        config.spot832,
        config.data832,
        logger=p_logger,
        max_wait_seconds=max_wait_seconds,
    )


@flow(name="prune_data832")
def prune_data832(relative_path: str):
    p_logger = get_run_logger()
    config = Config832()
    globus_settings = JSON.load("globus-settings").value
    max_wait_seconds = globus_settings["max_wait_seconds"]
    p_logger.info(f"Pruning {relative_path} from {config.data832}")
    prune_one_safe(
        relative_path,
        0,
        config.tc,
        config.data832,
        config.nersc832,
        logger=p_logger,
        max_wait_seconds=max_wait_seconds,
    )


# @flow(name="prune_many_data832")
# def prune_many_data832(project_dir_filter: str, older_than_days: int = 30, test: bool = False):
#     logger = get_run_logger()
#     config = Config832()
#     data_832_project_dirs = config.tc.operation_ls(config.data832.uuid, config.data832.root_path)
#     for project_dir in data_832_project_dirs:
#         if project_dir['name'] == project_dir_filter:
#             found_project = project_dir
#             break
#     assert found_project is not None, f"Project dir {project_dir_filter} not found."

#     logger.info(f"found project dir {project_dir_filter}")
#     data832_project_dir_path = config.data832.full_path(project_dir['name'])
#     nersc_project_dir_path = config.nersc832.full_path(project_dir['name'])
#     # produce a list of fiels that we know are in both data832 and NERSC. These
#     # can be safely deleted.
#     prunable_files = []
#     logger.info("getting files from data832")
#     data832_files_future =  get_files(config.tc, config.data832, data832_project_dir_path, [], older_than_days)
#     logger.info("getting files from nersc")
#     nersc_files_future =  get_files(config.tc, config.nersc832, nersc_project_dir_path, [], older_than_days)

#     data832_files = data832_files_future.result()
#     nersc_files = nersc_files_future.result()
#     logger.info(f"Found files. data832: {len(data832_files)}  nersc: {len(nersc_files)}")
#     for data832_file in data832_files:
#         nersc_file_analog = config.nersc832.root_path + data832_file.split(config.data832.root_path)[1]
#         if nersc_file_analog in nersc_files:
#             prunable_files.append(data832_file)
#     logger.info(f"pruning {len(prunable_files)} files")
#     if len(prunable_files) > 0 and not test:
#         prune_files(config.tc, config.data832, prunable_files)


if __name__ == "__main__":
    import sys
    import dotenv

    dotenv.load_dotenv()
    # prune_spot832(sys.argv[1], 36)
    prune_data832(sys.argv[1])
