# import logging
# import os

# from globus_sdk import TransferClient, TransferData
# import httpx

# from nersc_globus import NERSCGlobus
# from als_globus import ALSGlobus


# logger = logging.getLogger("data_mover.move_733_single")

# SPOT_733_DATA_ROOT = "/spot733-data/raw"


# class GlobusDataMover733:
#     def __init__(
#         self,
#         transfer_client: TransferClient,
#         data_733_endpoint,
#         nersc_endpoint,
#         dm_client: httpx.Client,
#     ):
#         self.transfer_client = transfer_client
#         self.als_733_endpoint: ALSGlobus = data_733_endpoint
#         self.data_733 = None
#         self.nersc_cori: NERSCGlobus = nersc_endpoint
#         self.nersc_cori.root_path = "/global/cfs/projectdirs/als/data_mover/7.3.3"
#         self.dm_client = dm_client

#     def initialize(self):
#         self.als_733_endpoint.initialize()
#         # self.data_733.initialize()
#         self.nersc_cori.initialize()


# def submit_transfer(transfer_client: TransferClient, source, dest, filename, label=""):
#     root_path = dest.root_path
#     tdata = TransferData(
#         transfer_client, source.id, dest.id, label, sync_level="checksum"
#     )
#     basename = os.path.basename(filename)
#     dirpath = os.path.dirname(filename)

#     # make directories
#     dirpath = dirpath.split("/")

#     if len(dirpath) > 2 and len(dirpath[0]) == 0 and dirpath[1] == "global":
#         dirpath = dirpath[2:]

#     new_path = root_path
#     for path in dirpath:
#         try:
#             if path == "userdata":
#                 path = "userdata_test"
#             new_path = new_path + "/" + path
#             transfer_client.operation_mkdir(dest.id, new_path)
#         except:
#             pass

#     tdata.add_item(filename + ".edf", new_path + "/" + basename + ".edf")
#     tdata.add_item(filename + ".txt", new_path + "/" + basename + ".txt")
#     breakpoint()
#     task = transfer_client.submit_transfer(tdata)
#     task_id = task["task_id"]
#     return task_id


# def copy_data733_to_nersc(job, transfer_wrapper: GlobusDataMover733):
#     spot733_path = job["path"]
#     tc = transfer_wrapper.transfer_client
#     filename = os.path.relpath(spot733_path, SPOT_733_DATA_ROOT)
#     task_id = submit_transfer(
#         transfer_wrapper.transfer_client,
#         transfer_wrapper.als_733_endpoint,
#         transfer_wrapper.nersc_cori,
#         filename,
#     )
#     logger.debug(f"transfer submitted: {spot733_path} globus_task_id: {task_id}")

#     count = 0
#     while not tc.task_wait(task_id) and count < 3:
#         task_response = tc.get_task(task_id)
#         logger.debug(f"task failed {task_response}")
#         status = task_response["status"]
#         if status == "ACTIVE":
#             continue
#         if status == "SUCCEEDED":
#             transfer_wrapper.dm_client.post(
#                 f"{job['id']}/state", json={"state": "transferred to nersc"}
#             )
#         else:
#             transfer_wrapper.dm_client.post(
#                 f"{job['id']}/state", json={"state": "NERSC transfer failed"}
#             )
#         return task_id


# def run_733(job, transfer_wrapper: GlobusDataMover733):
#     log_prefix = f"{job.get('id')}: {job.get('path')}"
#     logger.debug(f"{log_prefix} Got job ")
#     states = job["state"]
#     # timestamps = job["timestamp"]

#     if "all_transferred" in states:
#         logger.debug(f"{log_prefix} all transferred")
#         return

#     if "nersc_transfer_complete" in states:
#         logger.debug(f"{log_prefix} transferred to nersc")
#         return

#     # note complete, let's transfer to NERSC
#     task_id = copy_data733_to_nersc(job, transfer_wrapper)


# globus bl733data-2017                    /userdata/Koishi/2022_04_01_insituSAXS/
# on spot733             /spot733-data/raw/userdata/Koishi/2022_04_01_insituSAXS/


# example SEM control file:
# <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
# <bl733-metadata>
#     <owner>Koishi</owner>
#     <dataset>20220404_091034_003_Mn0p124__2506</dataset>
#     <stage_date>2022-04-04T09:10:34.219-0700</stage_date>
#     <detector>
#         <name>2M</name>
#         <single>
#             <image>/spot733-data/raw/userdata/Koishi/2022_04_01_insituSAXS/003_Mn0p124__2506_2m.edf</image>
#             <metadata>/spot733-data/raw/userdata/Koishi/2022_04_01_insituSAXS/003_Mn0p124__2506_2m.txt</metadata>
#             <calibration_image>/spot733-data/raw/userdata</calibration_image>
#             <calibration_metadata>/spot733-data/raw/userdata</calibration_metadata>
#         </single>
#     </detector>
#     <keywords>
#     </keywords>
# </bl733-metadata>
