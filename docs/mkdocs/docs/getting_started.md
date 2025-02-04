# Getting Started

To use the full functionality of this project, there are a few additional steps required to get started. Some of our workflows are run at specific HPC facilities, which requires getting user accounts and project access. We also rely on Prefect, a data orchestration tool, to launch and monitor our jobs.

## HPC Access

We support running flows on a couple of DOE supercomputer facilities, ALCF and NERSC, and in the near future we will support OLCF as well. Since each facility has different hardware and software configurations, the steps for gaining access to and running code on these systems will vary.

---

### ALCF

We support running computation tasks at Argonne Leadership Compute Facility (ALCF) through Globus Compute. [This webinar](https://www.alcf.anl.gov/events/remote-workflows-alcf) provides a great overview to running remote workflows at ALCF and the tools that are available. To gain access to this system, you can [request an account](https://my.alcf.anl.gov/accounts/#/accountRequest).

Once you have an account, you can login to Polaris:

**SSH into Polaris**
	
Login to Polaris using the following terminal command:

```bash
ssh <your ALCF username>@polaris.alcf.anl.gov
```

Password: copy passcode from the MobilePASS+ app.

---

### NERSC

We schedule computing tasks at National Energy Research Scientific Computing Center (NERSC) by leveraging Docker, SLURM, and SFAPI. To get access to these systems [follow these instructions](https://docs.nersc.gov/accounts/).

Once you have an account, you can follow along [here to see how to schedule remote tomography reconstructions at NERSC](nersc832.md).

___

### OLCF

- [Request access and login to OLCF.](https://my.olcf.ornl.gov/login)

- [OLCF user documentation.](https://docs.olcf.ornl.gov/index.html)
___


## Globus

We use [Globus](https://www.globus.org/) for two main purposes:

1.  High-speed file transfer
2.  Scheduling and running compute jobs

[Follow these instructions](https://www.globus.org/get-started) to get started with Globus.

### Confidential Client

Once you have a Globus account, you can create a `Confidential Client`. This generates a unique ID and key for authenticating with Globus through the API, avoiding a manual authentication step redirecting to the web login page. 

**Use an existing Globus Confidential Client, or create a new one**

- In your browser, navigate to globus.org
- Login
- On the left, navigate to "Settings"
- On the top navigation bar, select "Developers" 
- On the right under Projects, create a new project called Splash Flows or select an existing one
- Create a new registered app called Splash Flows App, or select an existing one
- Generate a secret
- Store the confidential client UUID and Secret (note: make sure you copy the Client UUID and **not** the Secret UUID)

**Note:** For each service client, you will need to get permissions set by the correct person at ALCF, NERSC or other facility to be able to use it for transfer endpoints depending on how guest collections are configured.


### Globus Transfer

Globus allows us to move data between computers at the ALS, and also to transfer endpoints at HPC facilities. Transferring data via Globus can be done in their web user interface, as well as in Python via their API.


### Globus Compute

For some data workflows we use Globus Compute to easily execute our code at an HPC facility such as ALCF. This requires configuring a [globus-compute-endpoint](https://globus-compute.readthedocs.io/en/stable/endpoints/endpoints.html) on the HPC system that listens for incoming compute tasks and schedules those jobs as soon as resources are available.  

Learn how to get Globus Compute set up at [ALCF to run tomography reconstructions](alcf832.md).

___

## Prefect

[Prefect](https://docs.prefect.io/latest/) is a workflow orchestration tool that allows us to register Python functions as "Tasks" and "Flows" that can be called, scheduled, and monitored nicely within their web user interface.

There are several approaches to using Prefect. 

1. Prefect Cloud
    * Good for local development and debugging.
    * Less ideal for a production environment.
2. Your own Prefect server deployment
    * Scalable, but you are responsible for making sure the server is running.
    * Ideal for production environments where you are launching Docker containers.

Here is an example of the [service_configs for our Prefect server](https://github.com/als-computing/service_configs).

### Login to Prefect in your environment

Before running or registering any flows, you must ensure you are authenticated with your Prefect server. Depending on your setup, do one of the following:

- **For Prefect Cloud:**  
  Run the following command and follow the instructions to authenticate:
  
    ```bash
    prefect cloud login
    ```
  
    or you can run the following to set your env variables directly:
    
    ```
    prefect config set PREFECT_API_URL="[API-URL]"    
    ```

    ```
    prefect config set PREFECT_API_KEY="[API-KEY]"
    ```
 
- **For self-hosted Prefect server:**
    
    Please refer to [the official documentation](https://docs.prefect.io/v3/manage/self-host).


### Register a Flow

You can run the following scripts to register the Prefect Flows in this project to the Prefect server configured in your environment.

- ```./create_deployment_832_dispatcher.sh```

- ```./create_deployments_7012.sh```

- ```./create_deployments_832.sh```

- ```./create_deployments_832_alcf.sh```

- ```./create_deployments_832_nersc.sh```

### Start a Agents/Workers

Prefect Agents are defined in the `create_deployments.sh` scripts, and can be started like this:

    prefect agent start --pool "alcf_flow_pool"

This creates an agent that listens to jobs on the `alcf_flow_pool` "Work Pool". In this way, each one of our deployed Prefect Flows can be associated with a specific Work Pool, allowing fine-grained control over the system.

### Call a Flow

Once a flow is registered as a deployment and your agent is running, you can trigger a run in several ways:

- **Via the Prefect UI**
    
    Navigate to your Prefect server dashboard, locate the registered flow deployment, and trigger a run manually.

- **Via the Command Line Interface**
   
    You can also trigger a run using the Prefect CLI:

        prefect deployment run <FLOW_NAME>/<DEPLOYMENT_NAME>

    Replace `<FLOW_NAME>/<DEPLOYMENT_NAME>` with the name of the deployment you want to run (e.g., `alcf_recon_flow/alcf_recon_flow`).

- **Via API Calls**

    For automation purposes, you may also call the Prefect API from your own scripts or applications using the Python SDK

        run_deployment(
            name="my-first-flow/my-first-deployment",
            parameters={"my_param": "42"},
            job_variables={"env": {"MY_ENV_VAR": "staging"}},
            timeout=0, # don't wait for the run to finish
        )
