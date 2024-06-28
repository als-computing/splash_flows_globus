# 8.3.2 ALCF Globus Flow And Reconstruction Setup

This script facilitates the transfer and processing of data files from the Advanced Light Source (ALS) at Berkeley Lab to the Argonne Leadership Computing Facility (ALCF) and the National Energy Research Scientific Computing Center (NERSC). It includes functions for transferring data using Globus and for performing tomographic reconstruction using Tomopy at ALCF.

Follow these steps to log in to ALCF Polaris, start a Globus compute endpoint signed in using a Globus confidential client, register a reconstruction function and flow, and run the flow using `ALCF_tomopy_reconstruction.py`.

## Outline:

### Data flow diagram
```mermaid
graph TD
    A[Beamline 8.3.2] -->|Collect Data| B[spot832]
    B -->|Transfer to NERSC| C[ /global/cfs/cdirs/als/data_mover/8.3.2/raw/< proposal folder name convention>/< h5 files>]
    C -->|Start This Prefect Flow| D[ start ALCF_tomopy_reconstruction.py ]
    D -->|Step 1: Transfer from NERSC to ALCF| E[ /eagle/IRIBeta/als/bl832/raw/< proposal folder name convention>/< h5 files>]
    E -->|Step 2: Tomography Reconstruction Globus Flow| F[ run reconstruction.py on ALCF globus-compute-endpoint]
    F -->|Save Reconstruction on ALCF| G[ /eagle/IRIBeta/als/bl832/scratch/< proposal folder name convention>/rec< dataset>/< tiffs> ]
    G -->|Step 3. Transfer back to NERSC| H[ /global/cfs/cdirs/als/data_mover/8.3.2/scratch/< folder name convention>/rec< dataset>/< tiffs>]
	
	classDef compute fill:#80CED7,stroke:#3B6064,stroke-width:4px;
	class D,F compute;
    
    classDef storage fill:#63C7B2,stroke:#3B6064,stroke-width:4px;
    class C,E,G,H storage;
	 
	classDef collection fill:#FFDDC1,stroke:#3B6064,stroke-width:4px;
    class A,B collection;
    
  ```

### File flow details
**Naming conventions**
-   Proposal folder name:
	- `< Proposal Prefix>-< 5 digit proposal number>_< first part of email address>/`
-   h5 files in experiment folder:
	- `< YYYYMMDD>_< HHMMSS>_< user defined string>.h5`
-   h5 files for tiled scans:
	- `< YYYYMMDD>_< HHMMSS>_< user defined string>_x<##>y<##>.h5`

**NERSC Raw Location**
-  `/global/cfs/cdirs/als/data_mover/8.3.2/raw/< proposal folder name convention>/< h5 files>`

**ALCF Raw Destination**
- `/eagle/IRIBeta/als/bl832/raw/< proposal folder name convention>/< h5 files>`

**ALCF Recon Destination**
- `/eagle/IRIBeta/als/bl832/scratch/< proposal folder name convention>/rec< dataset>/< tiffs>`

**NERSC Destination**
-  `/global/cfs/cdirs/als/data_mover/8.3.2/scratch/< proposal folder name convention>/rec< dataset>/< tiffs>`
	-   Prune reconstruction (NERSC) after 1 week

**Next steps:**
-   ALS Destination (data832)
	-   `/data/scratch/globus_share/< folder name convention>/rec< dataset>/< tiffs>`
	-   Purge this too – quickly … 3 days. Keep a close eye on space
-   .zarr generation as soon as there’s a folder of tiffs/reconstruction is done
	- determine where this will happen


## Requirements:

Some of the steps refer to code and notebooks provided in the git repository `als-computing/als_at_alcf_tomopy_compute`. Download/clone this repository as it will come in handy:

 - https://github.com/als-computing/als_at_alcf_tomopy_compute

Specifically, from here you will need: `Tomopy_for_ALS.ipynb`, and `template_config.yaml`.

Additionally, if you do not already have an ALCF account, follow the steps here to request access:
- https://docs.alcf.anl.gov/account-project-management/accounts-and-access/user-account-overview/

You will also need to update the included `.env.example` file (rename it `.env`) with particular endpoints, IDs, and secrets, which are annotated below along with the steps they correspond to:

### **.env.example**
```python
# Used by Python script to sign into Globus using confidential client
GLOBUS_CLIENT_ID="< Client UUID from step 3 >"
GLOBUS_CLIENT_SECRET="< Client Secret from step 3"
# These set environment variables to sign into globus-compute-endpoint environment using confidential client
GLOBUS_COMPUTE_CLIENT_ID="< Client UUID from step 3 >"
GLOBUS_COMPUTE_CLIENT_SECRET="< Client Secret from step 3"
# globus-compute-endpoint UUID
GLOBUS_COMPUTE_ENDPOINT="< globus-compute-endpoint ID from step 6 >"
# Reconstruction function and flow ID generated in Tomopy_for_ALS.ipynb 
GLOBUS_RECONSTRUCTION_FUNC="< Reconstruction function id from step 8 >"
GLOBUS_FLOW_ID="< Flow ID from step 9 >"
GLOBUS_IRIBETA_CGS_ENDPOINT="< IRIBeta_als guest collection UUID on Globus >"
```

## On Polaris:
1. **SSH into Polaris**
	
	Login to Polaris using the following terminal command:
	
		ssh <your ALCF username>@polaris.alcf.anl.gov

	Password: copy passcode from MobilePASS+

2. **Copy and Update endpoint template_config.yaml file**

	 From the `als-computing/als_at_alcf_tomopy_compute` repository, copy `template_config.yaml` into your home directory on Polaris (`eagle/home/<your ALCF username>`).

	Ex: using vim on Polaris
	- `vim template_config.yaml`
	- Paste file contents once the editor opens up
	- To save, type:
		- `:wq!` and press enter.
	- The view should switch back to the command line.

	**Note:** template_config.yaml needs to be updated to account for the new version of globus-compute-endpoint:
  
	What you need to change is that you should replace this:  
	```
	strategy:
	    max_idletime: 300
	    type: SimpleStrategy
	type: GlobusComputeEngine
	```
	with this:  
	
	```
	strategy: simple
	job_status_kwargs:
	    max_idletime: 300
	    strategy_period: 60
	type: GlobusComputeEngine
	```

	These new settings also introduce a parameter "strategy_period" that controls the frequency with which the endpoint checks pbs for completed jobs. The default was 5 seconds, and this change makes it 60 seconds.

3. **Use an existing Globus Confidential Client, or create a new one**

	- In your browser, navigate to globus.org
	- Login
	- On the left, navigate to "Settings"
	- On the top navigation bar, select "Developers" 
	- On the right under Projects, select "IRIBeta" (or create a new project)
	- Select a registered app (`dabramov ALCF Globus Flows Test`), or create a new one
	- Generate a secret
	- Store the confidential client UUID and Secret (note: make sure you copy the Client UUID and **not** the Secret UUID)

	**Note:** If you create a new service client, you will need to get permissions set by the correct person at ALCF and NERSC to be able to use it for transfer endpoints.
	
4. **Log into globus-compute-endpoint on Polaris with the service confidential client**

	In your terminal on Polaris, set the following global variables with the Globus Confidential Client UUID and Secret respectively. Check the documentation here for more information (https://globus-compute.readthedocs.io/en/stable/sdk.html#client-credentials-with-clients). Globus-compute-endpoint will then log in using these credentials automatically:

		export GLOBUS_COMPUTE_CLIENT_ID="<UUID>"
		export GLOBUS_COMPUTE_CLIENT_SECRET="<SECRET>"
	
	**Note**: you can make sure you are signed into the correct account by entering:

		globus-compute-endpoint whoami

	If you are signed into your personal Globus account, make sure to sign out completely using:

		globus-compute-endpoint logout

5. **Start globus-compute `tomopy` environment and activate the endpoint**
		On Polaris, enter the following commands to activate a Conda environment that has been set up to run reconstruction using `tomopy` and `globus-compute-endpoint`.
	```bash
	module  load  conda
	conda  activate  /eagle/IRIBeta/als/env/tomopy
	globus-compute-endpoint  configure  --endpoint-config  template_config.yaml  als_endpoint
	globus-compute-endpoint  start  als_endpoint
	globus-compute-endpoint  list
	```		
	This will create an endpoint and display its status. Its status should be listed as `running`. There will also be displayed a unique Endpoint ID in the form of a UUID.
	
6. **Store endpoint uuid in .env file**

	Store the Endpoint UUID in your `.env` file, which is given by running `globus-compute-endpoint list`. This will be used for running the Flow.

## In your local environment:

 7. **Create a new conda environment called `globus_env`, or install Python dependencies directly (install.txt)**

	The only requirement is a local environment, such as a Conda environment, that has Python 3.11 installed along with the Globus packages `globus_compute_sdk` and `globus_cli`. If you have a local installation of Conda you can set up an environment that can run the `Tomopy_for_ALS.ipynb` notebook and `ALCF_tomopy_reconstruction.py` with these steps in your terminal:

	```bash
	conda  create  -n  globus_env  python==3.11
	conda  activate  globus_env
	pip  install  globus_compute_sdk  globus_cli  python-dotenv
	```

	Note that the tomopy environment on Polaris contains Python 3.11. It is therefore necessary for this environment on your local machine to have a Python version close to this version.

8. **Generate `GLOBUS_RECONSTRUCTION_FUNC` (store in `.env`)**

	Follow the `Tomopy_for_ALS.ipynb` notebook until you get to the "Register Function" cell:

	```python
    reconstruction_func = gc.register_function(reconstruction_wrapper)
    print(reconstruction_func)
    ```
	Save the UUID that is printed out in your `.env` file.
	
		GLOBUS_RECONSTRUCTION_FUNC="< UUID >"

9. **Generate `GLOBUS_FLOW_ID` (store in `.env`)**

	Continue following `Tomopy_for_ALS.ipynb` until you get to the cell before "Run the Flow":
	```python
	flow = fc.create_flow(definition=flow_definition,  title="Reconstruction flow",  input_schema={})
	flow_id = flow['id']
	print(flow)
	flow_scope = flow['globus_auth_scope']
	print(f'Newly created flow with id:\n{flow_id}\nand scope:\n{flow_scope}')
	```
	Save the flow_id in your `.env` file.


 10. **Prefect server**

		Make sure you have Prefect setup. If you want to connect to a particular Prefect server that is already running, then in your local terminal set `PREFECT_API_URL` to your desired server address:

			prefect  config  set  PREFECT_API_URL="http://your-prefect-server/"

		Otherwise, you can start a local server by running:
		
			prefect server start

 11. **Run the script from the terminal:**
		
			python -m orchestration.flows.bl832.ALCF_tomopy_reconstruction

		Monitor the logs in the terminal, which will update you on the current status.
		Step 1: Transfer data from NERSC to ALCF
		Step 2: Run the reconstruction Globus Flow, save to ALCF
		Step 3: Transfer reconstruction to NERSC

		Errors at step 1 or 3?
		- It may be authentication issues with your transfer client. Did you set `GLOBUS_CLIENT_ID` and `GLOBUS_CLIENT_SECRET` in `.env`?
		- It could also be permissions errors. Did you make a new service client? You will need to request permissions for the endpoints you are transferring to and from (both ALCF and NERSC).

		Errors at step 2?
		- It may be that your compute endpoint stopped!
		
			- You can check on Polaris using:
					
					globus-compute-endpoint list
					
			- If your endpoint is listed with the status "Stopped," you can restart using:
					
					globus-compute-endpoint start < your endpoint name >
			 
		- It may be authentication issues with your confidential client. Did you set the environment variables in your terminal on Polaris?

				export GLOBUS_COMPUTE_CLIENT_ID="<UUID>"
				export GLOBUS_COMPUTE_CLIENT_SECRET="<SECRET>"
			
			You can also check if you are logged into the confidential client:
			
				globus-compute-endpoint whoami
12. **Check if the flow completed successfully**

	If the data successfully transferred back to NERSC, you will find it in the following location:
	`/global/cfs/cdirs/als/data_mover/8.3.2/scratch/< folder name convention>/rec< dataset>/< tiffs>`