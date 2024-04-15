import os
from datetime import datetime
from collections import OrderedDict

cdtools_parms = OrderedDict(
    {
        "run_split_reconstructions": False,
        "n_modes": 1,
        "oversampling_factor": 1,
        "propagation_distance": 50 * 1e-6,
        "simulate_probe_translation": True,
        "n_init_rounds": 1,
        "n_init_iter": 50,
        "n_final_iter": 50,
        "translation_randomization": 0,
        "probe_initialization": None,
        "init_background": False,
        "probe_support_radius": None,
    }
)

ptychocam_parms = OrderedDict(
    {
        "n_iter": 500,
        "period_illu_refine": 0,
        "period_bg_refine": 0,
        "use_illu_mask": False,
    }
)


def create_job_script(path_job_script, n_gpu, args, time=4, nodes=1):
    now = datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    jobpath = os.path.join(path_job_script, "%s.txt" % time_str)
    with open(jobpath, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("#SBATCH --constraint=gpu\n")
        f.write("#SBATCH --gpus=%d\n" % n_gpu)
        f.write("#SBATCH --time=%s:00:00\n" % (str(time).zfill(2)))
        f.write("#SBATCH --nodes=%d\n" % nodes)
        f.write("#SBATCH --qos=regular\n")
        f.write("#SBATCH --account=als_g\n")
        f.write("%s" % (args))
    return jobpath


def get_job_script(path_job_script, n_gpu, args):
    job_path = create_job_script(path_job_script, n_gpu, args)
    with open(job_path, "r") as f:
        job_string = f.read()
    return job_string


def cdtool_args_string(cxiname, path_sh, orderParm, **kwargs):
    parms = orderParm.copy()
    for k, v in kwargs.items():
        if k in list(parms.keys()):
            parms[k] = v

    args = [path_sh] + [cxiname] + [v for k, v in parms.items()]
    args_string = " ".join(map(str, args))
    return args_string


def ptychocam_args_string(cxiname, path_sh, orderParm, **kwargs):
    parms = orderParm.copy()
    for k, v in kwargs.items():
        if k in list(parms.keys()):
            parms[k] = v

    args = "-i "
    args += f"{parms['n_iter']} "

    if parms["period_illu_refine"] != 0:
        args += "-r "
        args += f"{parms['period_illu_refine}']} "
    if parms["period_bg_refine"] != 0:
        args += "-T "
        args += f"{parms['period_bg_refine}']} "
    if parms["use_illu_mask"]:
        args += "-M "

    args += cxiname
    args_string = f"{path_sh} {args}"

    return args_string
