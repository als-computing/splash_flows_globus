# 8.3.2 ALCF Globus Flow And Reconstruction Setup

This script facilitates the transfer and processing of data files from the Advanced Light Source (ALS) at Berkeley Lab to the Argonne Leadership Computing Facility (ALCF) and the National Energy Research Scientific Computing Center (NERSC). It includes functions for transferring data using Globus and for performing tomographic reconstruction using Tomopy at ALCF.

Follow these steps to log in to ALCF Polaris, start a Globus compute endpoint signed in using a Globus confidential client, register a reconstruction function and flow, and run the flow using `orchestration/flows/bl832/alcf.py`.

# Details
## Data flow diagram
```mermaid
%%{
init: {
  "theme": "base",
  "themeVariables": {
    "primaryColor": "#000000",
    "primaryTextColor": "#000000",
    "primaryBorderColor": "#000000",
    "lineColor": "#808080", // Gray arrows
    "arrowheadColor": "#ffffff",
    "secondaryColor": "#00000",
    "tertiaryColor": "#ffffff",
    "fontFamily": "Arial, Helvetica, sans-serif",
    "background": "#ffffff",
  }
}
}%%
graph TD
    A[Beamline 8.3.2] -->|Collect Data| B[spot832]
    B -->|Transfer to NERSC| C[ /global/cfs/cdirs/als/data_mover/8.3.2/raw/< experiment folder name convention>/< h5 files>]
    C -->|Start This Prefect Flow| D[ python -m orchestration.flows.bl832.alcf ]
    D -->|Step 1: Transfer from NERSC to ALCF| E[ /eagle/IRIBeta/als/bl832/raw/< experiment folder name convention>/< h5 files>]
    E -->|Step 2: Tomography Reconstruction Globus Flow| F[ run reconstruction.py and tiff_to_zarr.py on ALCF globus-compute-endpoint]
    F -->|Save Reconstruction on ALCF| G[ /eagle/IRIBeta/als/bl832/scratch/< experiment folder name convention>/rec< dataset>/< tiffs> <br> /eagle/IRIBeta/als/bl832/scratch/< experiment folder name convention>/rec< dataset>.zarr/ ]
    G -->|Step 3: Transfer back to NERSC| J[ /global/cfs/cdirs/als/data_mover/8.3.2/scratch/< experiment folder name convention>/rec< dataset>/< tiffs> <br> /global/cfs/cdirs/als/data_mover/8.3.2/scratch/< experiment folder name convention>/rec< dataset>.zarr/ ]
    J -->|Schedule Pruning| L[Prune raw and scratch data from ALCF and NERSC]
    J --> M[Visualize Results] 
    J -->|SciCat| N[TODO: ingest metadata into SciCat]
    M -->|2D Image Slices| O[view .tiff images ]
    M -->|3D Visualization| P[load .zarr directory in view_tiff.ipynb]

	classDef storage fill:#A3C1DA,stroke:#A3C1DA,stroke-width:2px,color:#000000;
	class C,E,G,J storage;

	classDef collection fill:#D3A6A1,stroke:#D3A6A1,stroke-width:2px,color:#000000;
	class A,B collection;

	classDef compute fill:#A9C0C9,stroke:#A9C0C9,stroke-width:2px,color:#000000;
	class D,F,K,L,N compute;

	classDef visualization fill:#E8D5A6,stroke:#E8D5A6,stroke-width:2px,color:#000000;
	class M,O,P visualization;
```

## File flow details
**Naming conventions**
-   Experiment folder name:
	- `< Proposal Prefix>-< 5 digit proposal number>_< first part of email address>/`
-   h5 files in experiment folder:
	- `< YYYYMMDD>_< HHMMSS>_< user defined string>.h5`
-   h5 files for tiled scans:
	- `< YYYYMMDD>_< HHMMSS>_< user defined string>_x<##>y<##>.h5`

**NERSC Raw Location**
-  `/global/cfs/cdirs/als/data_mover/8.3.2/raw/< Experiment folder name convention>/< h5 files>`

**ALCF Raw Destination**
- `/eagle/IRIBeta/als/bl832/raw/< Experiment folder name convention>/< h5 files>`

**ALCF Recon Destination**
- `/eagle/IRIBeta/als/bl832/scratch/< Experiment folder name convention>/rec< dataset>/< tiffs>`

**NERSC Destination**
-  `/global/cfs/cdirs/als/data_mover/8.3.2/scratch/< Experiment folder name convention>/rec< dataset>/< tiffs>`

## Globus Compute Flow

The preferred method for scheduling jobs on ALCF is through Globus Compute. This `reconstruction_wrapper()` function contains the code registered with Globus, which runs the reconstruction on ALCF. The commands call two Python programs located in the IRIBeta project folder on Polaris.
```python
def  reconstruction_wrapper(rundir,  h5_file_name,  folder_path):
	...
	# Run reconstruction.py
	# Output: A folder of tiff images
    command =  f"python /eagle/IRIBeta/als/example/globus_reconstruction.py {h5_file_name}  {folder_path}"
    recon_res = subprocess.run(command.split("  "),  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
	...
	# Run tiff_to_zarr.py
	# Output: .zarr directory
    file_name = h5_file_name[:-3]  if h5_file_name.endswith('.h5')  else h5_file_name
    command =  f"python /eagle/IRIBeta/als/example/tiff_to_zarr.py /eagle/IRIBeta/als/bl832_test/scratch/{folder_path}/rec{file_name}/"  # --zarr_directory /path/to/storage/"
    zarr_res = subprocess.run(command.split("  "),  stdout=subprocess.PIPE,  stderr=subprocess.PIPE)
```
### Reconstruction

The `globus_reconstruction.py` script in this flow is a slightly modified version of Dula Parkinson's tomography reconstruction code.  The modifications update the function's input parameters and set proper file/folder permissions for the reconstructed data.

Location on Polaris: `/eagle/IRIBeta/als/example/globus_reconstruction.py`

### Zarr Generation

Once the raw data has been reconstructed into a series of .tiff images, the script `tiff_to_zarr.py` transforms the images into a multiresolution .zarr directory, enabling 3D visualization. This code was adapted from the `view_tiff.ipynb` notebook and converted into a Python module. 

Polaris: `/eagle/IRIBeta/als/example/tiff_to_zarr.py`

## Pruning

After reconstruction is complete and data has moved back to NERSC/ALS, Prefect flows are scheduled to delete raw scratch paths at each location after a few days. Make sure to move the reconstructed data elsewhere before pruning occurs.

- **ALCF** 
	- Purge `raw` and `scratch` after 2 days
- **NERSC**
	- Purge `scratch` after 7 days
- **data832**
	- TODO:
		- Add ALS Destination (data832)
		- `/data/scratch/globus_share/< folder name convention>/rec< dataset>/< tiffs>`
		- `/data/scratch/globus_share/< folder name convention>/rec< dataset>.zarr/`
		- Purge `scratch` after 3 days

# Set up

A few steps are required to configure the environments locally and on ALCF Polaris.

## Requirements:

Some of the steps refer to code and notebooks provided in the git repository `als-computing/als_at_alcf_tomopy_compute`. Download/clone this repository as it will come in handy:

 - https://github.com/als-computing/als_at_alcf_tomopy_compute

Specifically, from here you will need: `Tomopy_for_ALS.ipynb`, and `template_config.yaml`.

Additionally, if you do not already have an ALCF account, follow the steps here to request access:
- https://docs.alcf.anl.gov/account-project-management/accounts-and-access/user-account-overview/

If you have not used Prefect before, it may be worthwhile to familiarize yourself with the documentation:
- https://docs.prefect.io/latest/

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
		On Polaris, enter the following commands to activate a Conda environment configured to run reconstruction using `tomopy` and `globus-compute-endpoint`.
	```bash
	module use /soft/modulefiles
	module  load  conda
	conda  activate  /eagle/IRIBeta/als/env/tomopy
	globus-compute-endpoint  configure  --endpoint-config  template_config.yaml  als_endpoint
	globus-compute-endpoint  start  als_endpoint
	globus-compute-endpoint  list
	```		
	This will create an endpoint and display its status. Its status should be listed as `running`. There will also be displayed a unique Endpoint ID in the form of a UUID.

	**Optional**: Create a new file called `activate_tomopy.sh`	in your home directory on Polaris and copy the following code:
	```bash
	#!/bin/bash
	#Load module files
	module use /soft/modulefiles
	#Load conda
	module load conda
	#initialize conda for the active session
	source $(conda info --base)/etc/profile.d/conda.sh
	#Activate conda environment
	conda activate /eagle/IRIBeta/als/env/tomopy
	#Print message
	echo "Conda environment '/eagle/IRIBeta/als/env/tomopy' activated successfully."
	```
	
	Now you can call this script using `source activate_tomopy.sh` in your home directory on Polaris to activate the environment in a single line of code. Be aware that this will not configure or start any globus-compute-endpoints.
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
		
			python -m orchestration.flows.bl832.alcf

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
		
		/global/cfs/cdirs/als/data_mover/8.3.2/scratch/< folder name convention>/rec< dataset>/< tiffs>



## Schedule Pruning with Prefect Workers

Following the previous steps to set up the Polaris and local environments enables `alcf.py` to properly run reconstruction and transfer data, however, additional steps are necessary for the file pruning to be scheduled and executed. This relies on Prefect Flows, Deployments, Work Pools, and Queues.

### Terminology
- **Flow**: Define a Prefect Flow in your code using the `@flow(name="flow_name")` decorator. A flow encapsulates a series of tasks to be executed.
- **Deployment**: Deploy your Flow to your Prefect server, and assign it a work pool and queue. When called, deployments will create a flow run and submit it to a specific work pool for scheduling.
- **Work Pool**: Organizes workers depending on infrastructure implementation. Work pools manage how work is distributed among workers. For more information, check the documentation: https://docs.prefect.io/latest/concepts/work-pools/
- **Queue**: Each work pool has a "default" queue where work will be sent, and multiple queues can be added for fine-grained control over specific processes (i.e. setting priority and concurrency for different pruning functions).
- **Workers**: Formerly known as Agents. Can be assigned a specific Work Pool and Queue, and will listen to incoming jobs from those channels.

### Pruning Example

This is an example of a pruning flow found in `orchestration/flows/bl832/prune.py`:

```python
@flow(name="prune_alcf832_raw")
def  prune_alcf832_raw(relative_path:  str):
	
	p_logger = get_run_logger()
	tc = initialize_transfer_client()
	config = Config832()
	globus_settings = JSON.load("globus-settings").value
	max_wait_seconds = globus_settings["max_wait_seconds"]
	p_logger.info(f"Pruning {relative_path} from {config.alcf832_raw}")
	prune_one_safe(
		file=relative_path,
		if_older_than_days=0,
		tranfer_client=tc,
		source_endpoint=config.alcf832_raw,
		check_endpoint=config.nersc832_alsdev_raw,
		logger=p_logger,
		max_wait_seconds=max_wait_seconds,)
```

To schedule and execute this flow from `alcf.py`, run this shell script in your terminal:

	./create_deployments_832_alcf.sh

This script builds Prefect deployments for the different applicable prune functions in  `prune.py`, for example:
	
```bash
# create_deployments_832_alcf.sh
prefect  deployment  build  ./orchestration/flows/bl832/prune.py:prune_alcf832_raw  -n  prune_alcf832_raw  -q  bl832  -p  test_pool
prefect  deployment  apply  prune_alcf832_raw-deployment.yaml
``` 

Note: By default the deployments will not have access to the contents in the `.env` file, meaning the Globus confidential client ID and secret must be added using a different method. Instead, we can use Prefect Secret Blocks in the web UI to store  `GLOBUS_CLIENT_ID` and `GLOBUS_CLIENT_SECRET`, and securely import those into our script:

```python
from prefect.blocks.system import Secret

CLIENT_ID = Secret.load("globus-client-id")
CLIENT_SECRET = Secret.load("globus-client-secret")
# Access the stored secret
secret_block.get()
```

### Start a worker

In a new terminal window, you can start a new worker by executing the following command:

	prefect worker start -p test_pool -q bl832

TODO: describe how to do this in production

### Prefect Blocks

In the Prefect UI, navigate to the left column under "Configuration" and select "Blocks." Press the "+" button to add a new Block from the catalog. Search for "JSON" and create a new JSON Block called "pruning-config." This stores configuration information in a way that can be changed in the UI without adjusting the source code. For example:

```json
# pruning-config
{
	"delete_alcf832_files_after_days":2,
	"delete_data832_files_after_days":3,
	"delete_nersc832_files_after_days":7,
	"max_wait_seconds":120
}
```
Read more about Blocks here: https://docs.prefect.io/latest/concepts/blocks/


# Performance

### Preliminary results:
Trial 1
|**Task**|**Description**|**Duration (min)**|**Size (GB)**|
|-----------------|------------------------------------------------------|----|-----|
|**NERSC to ALCF**|`BLS-00564_dyparkinson/20230224_132553_sea_shell.h5`  |3.3 |8.51 |
|**Globus Flow**. |Full tomographic reconstruction and Zarr conversion   |22.9|N/A  |
|**ALCF to NERSC**|Transfer of reconstructed data (tiffs + Zarr) to NERSC|3.8 |55.29|

Trial 2
|**Task**|**Description**|**Duration (min)**|**Size (GB)**|
|-----------------|------------------------------------------------------|-----|-----|
|**NERSC to ALCF**|`BLS-00564_dyparkinson/20230224_132553_sea_shell.h5`  |0.31 |8.51 |
|**Globus Flow**. |Full tomographic reconstruction and Zarr conversion   |9.09|N/A  |
|**ALCF to NERSC**|Transfer of reconstructed data (tiffs + Zarr) to NERSC|0.90|55.29|

TODO: Investigate discrepancy in performance speed