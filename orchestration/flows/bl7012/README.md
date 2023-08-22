# Data movement BL7012
This package contains functions to initiate Prefect data processing workflows for COSMIC data. The current implemented Prefect workflow enables data movement from a Globus beamline endpoint to the desinated NERSC Globus endpoint. 

# Getting start
Set up a python enviornment as follow:
```
$   git clone https://github.com/grace227/splash_flows_globus.git
$   cd splash_flows_globus
$   pip install -e .
```
Provide information of a Globus collection endpoint in `splash_flows_globus/config.yml`:
```
globus:
  globus_endpoints:
    spot832:
      root_path: /
      uri: spot832.lbl.gov
      uuid: 44ae904c-ab64-4145-a8f0-7287de38324d
```
Provide information of Prefect and Globus authentication in `.env` file:
```
GLOBUS_CLIENT_ID=<globus_client_id>
GLOBUS_CLIENT_SECRET=<globus_client_secret>
PREFECT_API_URL=<url_of_prefect_server>
PREFECT_API_KEY=<prefect_client_secret>
```
Create a Prefect deployment workflow. Append new workflow in the `create_deployments.sh` file:
```
prefect deployment build <path_of_file>:<prefect_function> -n 'name_of_the_workflow' -q <tag>
prefect deployment apply <prefect_function>-deployment.yaml
```
Following example creates a Prefect workflow for the function of `process_new_file` in file of `./orchestration/flows/bl7012/move.py`
```
prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file -n 'process_newdata7012' -q bl7012
prefect deployment apply process_new_file-deployment.yaml
```

# Starting a Prefect workflow manually
Below is the command to start the Prefect workflow:
```
python -m orchestration.flows.bl7012.move <Relative path of file respect to the root_path defined in Globus endpoint>
```

# Submitting workflow via Prefect API
An example is shown `example.ipynb` to submit a PREFECT workflow to PREFECT server. 

Once the job is submitted, a workflow agent is needed to work on jobs in queue. A workflow agent can be launched by:
```
prefect agent start -q <name-of-work-queue>
```
It requires to have the `PREFECT_API_URL` and `PREFECT_API_KEY` stored as an environment variables, such that the agent knows where to get the work queue. Once the agent is launched, the following message indicates where the agent is currently listening to.
```
Starting v2.7.9 agent connected to https://.../api...

  ___ ___ ___ ___ ___ ___ _____     _   ___ ___ _  _ _____
 | _ \ _ \ __| __| __/ __|_   _|   /_\ / __| __| \| |_   _|
 |  _/   / _|| _|| _| (__  | |    / _ \ (_ | _|| .` | | |
 |_| |_|_\___|_| |___\___| |_|   /_/ \_\___|___|_|\_| |_|


Agent started! Looking for work from queue(s): <name-of-work-queue>...
```

# Deploy a Ptychograpy NERSC Prefect Agent
A Prefect Agent is process that exists to run the code in a Prefect Flow. This repository supports building a container that runs prefect agents for several beamline operations at NERSC. The bl7012 agent has code that can copy data to NERSC and luanch a reconstruction at NERSC. The reconstruction is performed at NERSC, and the agent uses the [Super Facility API](https://docs.nersc.gov/services/sfapi/) to launch reconstruction jobs.

The agent is very light weight. It does not run reconstruction code so it does not have be run on powerful hardware and does not require direct access to raw data.

The agent can be easily run as a `docker` or `podman` container. Here is an example of a `docker-compose.xml` file that launches the container:

```yaml
  agent:
    container_name: agent
    image: ghcr.io/als-computing/splash_flows_globus:main
    environment:
    - PREFECT_API_URL=http://prefect:8000/api  # address of the prefect server to poll and fetch jobs from
    - PREFECT_API_KEY=${PREFECT_API_KEY}  # api key to access the prefect server
    - GLOBUS_CLIENT_ID=${GLOBUS_CLIENT_ID} # globus client id to use to transfer data (using  https://docs.globus.org/api/auth/developer-guide/#developing-apps)
    - GLOBUS_CLIENT_SECRET=${GLOBUS_CLIENT_SECRET} # globus client secret
    # The following two items are paths where stored NERSC Id file is stored. This is a local file that contains the cleintit.txt generated from https://docs.nersc.gov/services/sfapi/authentication/#client. These must be kept secret and secure wherever your agent is running.
    - PATH_NERSC_ID=/nersc/clientid.txt  
    - PATH_NERSC_PRI_KEY=/nersc/priv_key.pem
    - PATH_JOB_SCRIPT=/tmp_job_scripts  # path where slurm script files are written. This is for debug only and may go away in future versions.
    - PATH_PTYCHOCAM_NERSC=/global/cfs/cdirs/als/data_mover/ptycho/cosmic_reconstruction_at_nersc/c_ptychocam/ptychocam_reconstruction.sh  # Path at NERSC where reconstruction script lives
    command: prefect agent start -q "transfer_auto_recon"  # listen to the queue for the desired deployment, in this case, the queue whee transfer_auto_recon scripts are written
    restart: unless-stopped
    logging:
      options:
        max-size: "1m"
        max-file: "3"
    volumes:
    - ./nersc:/nersc
    - ./tmp_job_scripts:/tmp_job_scripts
    networks:
      prefect:
```

## What's going on?

``` mermaid
---
title: Deployment
---
graph 


    bc[Beamline Computer] --new flow--> prefect_api

    subgraph ps [Prefect Server]

      prefect_api --> db1[(Database)]
    
    end
    
    subgraph Globus
      transfer --> nersc

    end 
    subgraph NERSC
      sfapi --> slurm
      slurm --> reconstruction_script
      reconstruction_script --> recon_container
    end

    subgraph container [Agent Container]
      agent <-- polling --> prefect_api
      agent -- Create --> flow_run
      flow_run --> flow_run_transfer
      flow_run_transfer --> transfer
      flow_run_transfer --> flow_run_reconstruct
      flow_run_reconstruct -- new job --> sfapi
      
    end
```

The flow is started by a beamline computer sending a call to the prefect server to create a flow run from a deployment. 

The `flow` [flow_auto_recon](https://github.com/als-computing/splash_flows_globus/blob/6591299497ad8a7e0fb3c811a16f44b827c39540/orchestration/flows/bl7012/move_recon.py#L184) performs two main task: it copies data to NERSC and kicks off a reconstruction at NERSC.

The move is performed via a Globus API call. Once the file is transferred, the `flow` kicks off a reconstruction sending a message to the SFAPI at NERSC to create a reconstruction job.
