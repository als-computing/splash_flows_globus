# Orchestration

A common architecture for moving data is defined in ```orchestration/transfer_controller.py```.

## bl7012

Info goes here

## bl733

Info goes here

## bl832

[Beamline 8.3.2](https://microct.lbl.gov/) is the Hard X-ray Micro-tomography instrument at the Advanced Light Source.

### `config.py`

The `Config832` class is designed to configure and initialize various components needed for data transfer and orchestration workflows related to the ALS beamline 832.

- **Initialization**:  
    The `Config832` class retrieves configuration data using the `transfer.get_config()` function, which is used to set up necessary endpoints and applications for data transfer.

- **Endpoints and Applications**:  
    It constructs a set of endpoints using `transfer.build_endpoints()` and applications with `transfer.build_apps()`. These endpoints represent different storage locations for data, both on local and remote systems (e.g., `spot832`, `data832`, `nersc832`, etc.).

- **Transfer Client**:  
    A `TransferClient` instance is created using `transfer.init_transfer_client()`, with a specified application (`als_transfer`). This client will handle the file transfer operations between different endpoints.

- **Storage Locations**:  
    Multiple endpoints related to data storage are stored as attributes, representing raw data, scratch space, and other storage locations for different systems (e.g., `data832_raw`, `nersc832_alsdev_scratch`).

- **Additional Configurations**:  
    The configuration dictionary (`config`) also contains other configuration values like `scicat` (for metadata) and `ghcr_images832` (for container images), which are also stored as attributes.

### `dispatcher.py`

This script is designed to automate and orchestrate the decision-making process for the BL832 beamline, ensuring that the appropriate workflows are triggered based on predefined settings.

#### Key Components:

- **`FlowParameterMapper` Class**:  
    This class is used to map the required parameters for each flow based on a predefined set of parameters. The `get_flow_parameters` method filters and returns only the relevant parameters for the specified flow based on a dictionary of available parameters.

- **`DecisionFlowInputModel`**:  
    A Pydantic model that validates input parameters for the decision flow, including the file path, export control status, and configuration dictionary. It ensures that the input parameters are in the correct format before they are used in the decision-making process.

- **`setup_decision_settings` Task**:  
    This task defines the settings for which flows should be executed (e.g., ALCF, NERSC, and 832 beamline file management). It logs the configuration and saves the decision settings as a JSON block for later use by other flows.

- **`run_specific_flow` Task**:  
    An asynchronous task that runs a specific flow based on the provided flow name and parameters. This task is dynamically executed and uses the Prefect deployment system to trigger flows as needed.

- **`dispatcher` Flow**:  
    The main flow, which coordinates the execution of various tasks:  
    1. Validates input parameters using `DecisionFlowInputModel`.  
    2. Loads decision settings from a previously saved JSON block.  
    3. Runs the `new_832_file_flow/new_file_832` flow synchronously first if required.  
    4. Runs ALCF and NERSC flows asynchronously if they are enabled in the decision settings.  
    This flow ensures that the correct sub-flows are executed in the correct order, using dynamic parameters for each flow.

### `move.py`

This script defines a set of tasks and flows for transferring files between different endpoints and systems, processing data, and managing file deletions. It leverages Globus for file transfers and Prefect for orchestration.

#### Key Components:

1. **`transfer_spot_to_data` Task**:  
    - This task transfers a file from the `spot832` endpoint to the `data832` endpoint using the `GlobusTransferClient`.  
    - It ensures that the file paths are correctly formatted and calls the `start_transfer` function to perform the transfer.

2. **`transfer_data_to_nersc` Task**:  
    - Similar to the previous task, this one transfers data from `data832` to the `nersc832` endpoint.  
    - The task also handles file path formatting and logs transfer details.

3. **`process_new_832_file` Flow**:  
    - This is the main flow for processing new files in the system. It performs multiple steps:  
        1. **Transfers** the file from `spot832` to `data832` using `transfer_spot_to_data`.  
        2. **Transfers** the file from `data832` to `NERSC` if export control is not enabled and `send_to_nersc` is true, using `transfer_data_to_nersc`.  
        3. **Ingests** the file into SciCat using the `ingest_dataset` function. If SciCat ingestion fails, it logs an error.  
        4. **Schedules** file deletion tasks for both `spot832` and `data832` using the `schedule_prefect_flow` function, ensuring files are deleted after a set number of days as configured in `bl832-settings`.

4. **`test_transfers_832` Flow**:  
    - This flow is used for testing transfers between `spot832`, `data832`, and `NERSC`. It:  
        1. Generates a new unique file name.  
        2. Transfers the file from `spot832` to `data832` and then from `data832` to `NERSC`.  
        3. Logs the success of each transfer and checks that the process works as expected.

#### Configuration:

- **`API_KEY`**: The API key is retrieved from the environment variable `API_KEY`, which is used for authorization with Globus and other services.
- **`TOMO_INGESTOR_MODULE`**: This is a reference to the module used for ingesting datasets into SciCat.
- **`Config832`**: The configuration for the beamline 832 environment, which includes the necessary endpoints for file transfers and other operations.

#### File Deletion Scheduling:

- After transferring the files, the flow schedules the deletion of files from `spot832` and `data832` after a predefined number of days, using Prefect's flow scheduling capabilities.

#### Error Handling:

- Throughout the script, errors are logged in case of failure during file transfers, SciCat ingestion, or scheduling tasks.

#### Use Case:

This script is primarily used for automating the movement of files from `spot832` to `data832`, sending data to `NERSC`, ingesting the data into SciCat, and managing file cleanup on both `spot832` and `data832` systems. It allows for flexible configuration based on export control settings and NERSC transfer preferences.

### ```job_controller.py```

This script defines an abstract class and factory function for managing tomography reconstruction and multi-resolution dataset building on different High-Performance Computing (HPC) systems. It uses the `Config832` class for configuration and handles different HPC environments like ALCF and NERSC. The abstraction allows for easy expansion to support additional systems like OLCF in the future.

#### Key Components:

1. **`TomographyHPCController` Class (Abstract Base Class)**:
    - This abstract base class defines the interface for tomography HPC controllers, with methods for performing tomography reconstruction and generating multi-resolution datasets. 
    - **Methods**:
        - **`reconstruct(file_path: str) -> bool`**:  
            Performs tomography reconstruction for a given file. Returns `True` if successful, `False` otherwise.
        - **`build_multi_resolution(file_path: str) -> bool`**:  
            Generates a multi-resolution version of the reconstructed tomography data for a given file. Returns `True` if successful, `False` otherwise.

2. **`HPC` Enum**:
    - An enum to represent different HPC environments, with members:
        - `ALCF`: Argonne Leadership Computing Facility
        - `NERSC`: National Energy Research Scientific Computing Center
        - `OLCF`: Oak Ridge Leadership Computing Facility (currently not implemented)

3. **`get_controller` Function**:
    - A factory function that returns an appropriate `TomographyHPCController` subclass instance based on the specified HPC environment (`ALCF`, `NERSC`, or `OLCF`).
    - **Parameters**:
        - `hpc_type`: An enum value identifying the HPC environment (e.g., `ALCF`, `NERSC`).
        - `config`: A `Config832` object containing configuration data.
    - **Returns**: An instance of the corresponding `TomographyHPCController` subclass.
    - **Raises**: A `ValueError` if the `hpc_type` is invalid or unsupported, or if no config object is provided.

### ```alcf.py```

This script is responsible for performing tomography reconstruction and multi-resolution data processing on ALCF using Globus Compute. It orchestrates file transfers, reconstructs tomography data, and builds multi-resolution datasets, then transfers the results back to the `data832` endpoint.

#### Key Components:

1. **`ALCFTomographyHPCController` Class**:
    - This class implements the `TomographyHPCController` abstract class for the ALCF environment, enabling tomography reconstruction and multi-resolution dataset creation using Globus Compute.
    - **Methods**:
        - **`reconstruct(file_path: str) -> bool`**:  
            Runs the tomography reconstruction using Globus Compute by submitting a job to ALCF.
        - **`build_multi_resolution(file_path: str) -> bool`**:  
            Converts TIFF files to Zarr format using Globus Compute.
        - **`_reconstruct_wrapper(...)`**:  
            A static method that wraps the reconstruction process, running the `globus_reconstruction.py` script.
        - **`_build_multi_resolution_wrapper(...)`**:  
            A static method that wraps the TIFF to Zarr conversion, running the `tiff_to_zarr.py` script.
        - **`_wait_for_globus_compute_future(future: Future, task_name: str, check_interval: int = 20) -> bool`**:  
            Waits for a Globus Compute task to complete and checks its status at regular intervals.

2. **`schedule_prune_task` Task**:
    - Schedules the deletion of files from a specified location, allowing the deletion of processed files from ALCF, NERSC, or data832.
    - **Parameters**:  
        - `path`: The file path to the folder containing files for deletion.  
        - `location`: The server location where the files are stored.  
        - `schedule_days`: The delay before deletion, in days.  
    - **Returns**:  
        `True` if the task was scheduled successfully, `False` otherwise.

3. **`schedule_pruning` Task**:
    - This task schedules multiple file pruning operations based on configuration settings. It takes paths for various scratch and raw data locations, including ALCF, NERSC, and data832.
    - **Parameters**:  
        - `alcf_raw_path`, `alcf_scratch_path_tiff`, `alcf_scratch_path_zarr`, etc.: Paths for various file locations to be pruned.  
        - `one_minute`: A flag for testing purposes, scheduling pruning after one minute.  
        - `config`: Configuration object for the flow.
    - **Returns**:  
        `True` if all tasks were scheduled successfully, `False` otherwise.

4. **`alcf_recon_flow` Flow**:
    - This is the main flow for processing and transferring files between `data832` and ALCF. It orchestrates the following steps:
        1. **Transfer data** from `data832` to ALCF.
        2. **Run tomography reconstruction** on ALCF using Globus Compute.
        3. **Run TIFF to Zarr conversion** on ALCF using Globus Compute.
        4. **Transfer the reconstructed data** (both TIFF and Zarr) back to `data832`.
        5. **Schedule pruning tasks** to delete files from ALCF and data832 after the defined period.

    - **Parameters**:  
        - `file_path`: The file to be processed, typically in `.h5` format.  
        - `config`: Configuration object containing the necessary endpoints for data transfers.

    - **Returns**:  
        `True` if the flow completed successfully, `False` otherwise.

### ```nersc.py```

This script manages tomography reconstruction and multi-resolution data processing on NERSC using the SFAPI client. It submits jobs for reconstruction and multi-resolution processing, transfers data between NERSC and data832, and schedules pruning tasks for file cleanup.

#### Key Components:

1. **`NERSCTomographyHPCController` Class**:
    - This class implements the `TomographyHPCController` abstract class for the NERSC environment, enabling tomography reconstruction and multi-resolution dataset creation.
    - **Methods**:
        - **`create_sfapi_client() -> Client`**:  
            Creates and returns a NERSC client instance using the provided credentials for accessing the NERSC SFAPI.
        - **`reconstruct(file_path: str) -> bool`**:  
            Starts the tomography reconstruction process at NERSC by submitting a job script to the Perlmutter machine using SFAPI.
        - **`build_multi_resolution(file_path: str) -> bool`**:  
            Converts TIFF files to Zarr format on NERSC by submitting a job script to Perlmutter.

2. **`schedule_pruning` Task**:
    - Schedules the deletion of files from various locations (data832, NERSC) after a specified duration.
    - **Parameters**:
        - `raw_file_path`, `tiff_file_path`, `zarr_file_path`: Paths to raw, TIFF, and Zarr files.
        - Uses configuration values to determine where to delete the files and when.
    - **Returns**:  
        `True` if all prune tasks were scheduled successfully, `False` otherwise.

3. **`nersc_recon_flow` Flow**:
    - This is the main flow for performing tomography reconstruction and multi-resolution processing at NERSC. It performs the following steps:
        1. **Run tomography reconstruction** using NERSC's SFAPI.
        2. **Run multi-resolution processing** to convert TIFF files to Zarr format.
        3. **Transfer the reconstructed TIFF and Zarr files** from NERSC to `data832`.
        4. **Schedule pruning tasks** to delete processed files from NERSC and data832 after a defined period.
    - **Parameters**:  
        - `file_path`: The path to the file to be processed.
        - `config`: Configuration object containing the necessary endpoints for data transfers.

    - **Returns**:  
        `True` if both reconstruction and multi-resolution tasks were successful, `False` otherwise.


### ```olcf.py```

To be implemented ...
