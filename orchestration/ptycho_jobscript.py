import os
from datetime import datetime
from collections import OrderedDict

default_path_client_id = "/global/software/ptycholive-dev/nersc_api_keys/clientid.txt"
default_path_private_key = "/global/software/ptycholive-dev/nersc_api_keys/priv_key.pem"
default_path_job_script = "/global/software/ptycholive-dev/ACME_Data_Cleaning_And_Assembly/src/acme_data_cleaning/nersc_job_script"
default_path_ptychocam_nersc = "~/cosmic_reconstruction_at_nersc/c_ptychocam/ptychocam_reconstruction.sh"
default_path_cdtools_nersc = "~/cosmic_reconstruction_at_nersc/c_cdtools/cdtools_reconstruction.sh"


cdtools_parms = OrderedDict({"run_split_reconstructions":False, "n_modes":1,
                 "oversampling_factor":1, "propagation_distance":50*1e-6,
                 "simulate_probe_translation":True, "n_init_rounds":1,
                 "n_init_iter":50, "n_final_iter":50, "translation_randomization":0,
                 "probe_initialization":None, "init_background":False,
                 "probe_support_radius":None})

ptychocam_parms = OrderedDict({"n_iter":500, "period_illu_refine":0,
                               "period_bg_refine":0, "use_illu_mask":False})


def create_job_script(n_gpu, args, time = 4, nodes = 1):
    now = datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    jobpath = os.path.join(default_path_job_script, '%s.txt'%time_str)
    with open(jobpath, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --constraint=gpu\n")
        f.write("#SBATCH --gpus=%d\n"%n_gpu)
        f.write("#SBATCH --time=%s:00:00\n"%(str(time).zfill(2)))
        f.write("#SBATCH --nodes=%d\n"%nodes)
        f.write("#SBATCH --qos=regular\n")
        f.write("#SBATCH --account=als_g\n")
        f.write("%s"%(args))
    return jobpath


def get_job_script(n_gpu, args):
    job_path = create_job_script(n_gpu, args)
    with open(job_path, 'r') as f:
        job_string = f.read()
    return job_string


def cdtool_args_string(cxiname, path_sh, orderParm, **kwargs):
    parms = orderParm.copy()
    for k, v in kwargs.items():
        if k in list(parms.keys()):
            parms[k] = v
    
    args = [path_sh]+[cxiname] + [v for k, v in parms.items()]
    args_string = ' '.join(map(str, args))
    return args_string

def ptychocam_args_string(cxiname, path_sh, orderParm, **kwargs):
    parms = orderParm.copy()
    for k, v in kwargs.items():
        if k in list(parms.keys()):
            parms[k] = v

    args = "-i "
    args += f"{parms['n_iter']} "

    if parms['period_illu_refine'] != 0:
        args += "-r "
        args += f"{parms['period_illu_refine}']} "
    if parms['period_bg_refine'] != 0:
        args += "-T "
        args += f"{parms['period_bg_refine}']} "
    if parms['use_illu_mask']:
        args += "-M "

    args += cxiname
    args_string = f"{path_sh} {args}"
    
    return args_string             


# default_path_job_script = "/global/software/ptycholive-dev/ACME_Data_Cleaning_And_Assembly/src/acme_data_cleaning/nersc_job_script"
# default_path_ptychocam_nersc = "~/cosmic_reconstruction_at_nersc/c_ptychocam/ptychocam_reconstruction.sh"
# default_path_cdtools_nersc = "~/cosmic_reconstruction_at_nersc/c_cdtools/cdtools_reconstruction.sh"
# default_path_ptycho_nersc = "~/cosmic_reconstruction_at_nersc/c_ptycho/ptycho_reconstruction.sh"

# def create_job_script(n_gpu, path_sh, args, time = 4, nodes = 1):
#     now = datetime.now()
#     time_str = now.strftime("%Y-%m-%d %H:%M:%S")
#     jobpath = os.path.join(default_path_job_script, '%s.txt'%time_str)
#     with open(jobpath, 'w') as f:
#         f.write("#!/bin/bash\n")
#         f.write("#SBATCH --constraint=gpu\n")
#         f.write("#SBATCH --gpus=%d\n"%n_gpu)
#         f.write("#SBATCH --time=%s:00:00\n"%(str(time).zfill(2)))
#         f.write("#SBATCH --nodes=%d\n"%nodes)
#         f.write("#SBATCH --qos=regular\n")
#         f.write("#SBATCH --account=als_g\n")
#         f.write("%s %s"%(path_sh, args))
#     return jobpath


# def get_job_script(n_gpu, path_sh, args):
#     job_path = create_job_script(n_gpu, path_sh, args)
#     with open(job_path, 'r') as f:
#         job_string = f.read()
#     return job_string


# cdtools_parms = OrderedDict({"run_split_reconstructions":False, "n_modes":1,
#                  "oversampling_factor":1, "propagation_distance":50*1e-6,
#                  "simulate_probe_translation":True, "n_init_rounds":1,
#                  "n_init_iter":50, "n_final_iter":50, "translation_randomization":0,
#                  "probe_initialization":None, "init_background":False,
#                  "probe_support_radius":None})

# def cdtools_args_string(cxiname, n_gpu = 1, path_sh = default_path_cdtools_nersc, **kwargs):
#     parms = cdtools_parms.copy()
#     for k, v in kwargs.items():
#         if k in list(parms.keys()):
#             parms[k] = v
    
#     args = [cxiname] + [v for k, v in parms.items()]
#     args_string = ' '.join(map(str, args))
#     return args_string


# # def cdtools(
# #     cxiname,

# # ):
# #     args = f"{cxiname} "
# #     args += f"{run_split_reconstructions} "
# #     args += f"{n_modes} "
# #     args += f"{oversampling_factor} "
# #     args += f"{propagation_distance} "
# #     args += f"{simulate_probe_translation} "
# #     args += f"{n_init_rounds} "
# #     args += f"{n_init_iter} "
# #     args += f"{n_final_iter} "
# #     args += f"{translation_randomization} "
# #     args += f"{probe_initialization} "
# #     args += f"{init_background} "
# #     args += f"{probe_support_radius}"

# #     # path_sh = '~/cosmic_reconstruction_at_nersc/c_cdtools/cdtools_reconstruction.sh'
# #     # n_gpu = 1
# #     # script_string = get_job_script_string(args, path_sh, n_gpu)

# #     script_string = f"""#!/bin/bash
# # #SBATCH --constraint=gpu
# # #SBATCH --gpus=1
# # #SBATCH --time=04:00:00
# # #SBATCH --nodes=1
# # #SBATCH --qos=regular
# # #SBATCH --account=als_g

# # ~/cosmic_reconstruction_at_nersc/c_cdtools/cdtools_reconstruction.sh {args}
# # """
# #     logger.info(f"Job script: {script_string}")

# #     task=submit_job_script_as_string(
# #         script_string=script_string,
# #         path_client_id=path_client_id,
# #         path_private_key=path_private_key
# #     )

# #     task_id = task['task_id']
# #     logger.info(f"Submitted task id: {task_id}")

# #     return task_wait(task_id, logger = logger)