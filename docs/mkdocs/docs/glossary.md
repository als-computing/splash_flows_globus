# Glossary

## ALCF
**ALCF**: The Argonne Leadership Computing Facility (ALCF) provides advanced computational resources for large-scale scientific research, offering systems like Aurora and Theta for high-performance computing.

---

## Docker
**Docker**: A platform for developing, shipping, and running applications in lightweight, portable containers. Docker containers isolate applications and their dependencies, making them easy to deploy and scale across different environments.

- **Dockerfile**: A text file that contains a series of instructions on how to build a Docker image. Each instruction in a Dockerfile creates a layer in the image, defining the environment and the application to be run. Dockerfiles are used to automate the creation of Docker images, ensuring consistency and reproducibility across different environments.
- **Image**: A Docker image is a read-only template that contains the instructions and filesystem layers needed to create a container. It’s like a blueprint. Images are immutable (cannot be changed once created), contain operating system dependencies, application code, libraries, environment variables, and metadata like default commands (via CMD or ENTRYPOINT). They can be built (via a Dockerfile), pulled from a registry (e.g., Docker Hub, private repositories), and shared between environments.
- **Container**: A Docker container is a runtime instance of a Docker image. Once the image is instantiated (started), it becomes a container. Containers are mutable at runtime (you can create/write files, run processes, etc.), and while they are isolated, they can interact with the host system (networking, mounts, etc.) as configured. Each container has its unique Container ID, its own filesystem layers (based on the image), and any ephemeral changes are lost unless committed as a new image or preserved using volumes.

---

## flake8
**flake8**: A Python tool for enforcing coding standards and style. Flake8 checks Python code for PEP 8 compliance, potential errors, and other common issues that could affect code quality and readability.

---

## Globus
**Globus** is a platform offering services for secure data transfer and remote compute across distributed systems.

- **Compute**: A service provided by Globus that allows users to execute workflows and processes on remote computing resources. It is often used for running large-scale jobs on distributed systems such as HPC facilities.
- **Transfer**: The service in Globus that enables the movement of data between endpoints. It handles secure, high-performance file transfer between various locations on the network.
- **Confidential Client**: A type of client used in Globus authentication systems. Confidential clients are trusted applications that can securely store credentials, unlike public clients that rely on user consent for access.
- **globus-compute-endpoint**: An endpoint used by Globus Compute to manage and execute remote computing jobs. These endpoints are configured to run workflows, tasks, and pipelines on distributed computing resources.

---

## HPC
**HPC (High Performance Computing)**:  
High Performance Computing (HPC) refers to the use of advanced computational resources, including supercomputers and computer clusters, to solve complex and computationally intensive problems. HPC systems enable parallel processing and high-speed calculations that are essential for research and development in scientific, engineering, and industrial fields. These systems help tackle large-scale simulations, data analyses, and modeling tasks that cannot be efficiently managed by standard computing setups.

---

## itk-vtk-viewer
**itk-vtk-viewer**: A visualization tool that integrates ITK (Insight Segmentation and Registration Toolkit) and VTK (Visualization Toolkit) to view 3D image data and volume renderings. It is commonly used for scientific and medical image processing.

---

## NERSC
**NERSC**: The National Energy Research Scientific Computing Center (NERSC) is a major supercomputing center for the U.S. Department of Energy’s Office of Science. NERSC supports a variety of scientific research projects and provides computational resources such as Cori and Perlmutter.

---

## OLCF
**OLCF**: The Oak Ridge Leadership Computing Facility (OLCF) provides some of the world's most powerful supercomputers to support scientific research. It includes systems such as Summit and Frontier, which enable simulations and modeling on a large scale.

---

## Prefect
**Prefect** is an orchestration framework for modern data workflows.

- **Agent**: A component of Prefect that listens for tasks and flows to be scheduled for execution. Agents are responsible for running and managing the tasks in a Prefect deployment.
- **Flow**: A collection of tasks in Prefect that define a workflow. Flows are the central part of Prefect's orchestration, encapsulating task dependencies, execution logic, and data flow.
- **Task**: An individual unit of work within a Prefect flow. Tasks can be executed in sequence or in parallel, depending on the dependencies defined in the flow.
- **Server**: The centralized control system in Prefect that manages flows, schedules, and task execution. The Prefect server stores metadata, logs, and state information for all flows and tasks.
- **Blocks**: A feature in Prefect that enables reusable components or functions within flows. Blocks can represent things like storage systems, APIs, or other tools that need to be configured and reused within different flows.
- **Deployments**: A deployment in Prefect refers to the configuration and setup of a flow to run in a specific environment. It includes details about the flow's parameters, the execution context, and deployment target.

---

## SciCat
**SciCat**: A scientific data management system designed for use in large research organizations and facilities. SciCat helps researchers store, organize, and track experimental data, metadata, and related information in a structured and accessible way.

---

## SFAPI
**SFAPI**: NERSC Superfacility Application Programming Interface (API). The API can be used to script jobs or workflows running against NERSC systems. The goal is to support everything from a simple script that submits a job to complex workflows that move files, check job progress, and make advanced compute time reservations.

---

## pytest
**pytest**: A framework for testing Python code, providing an easy-to-use interface for writing and running tests. Pytest supports a wide range of testing capabilities, including unit tests, integration tests, and fixture management.

---

## Tiled
**Tiled**: A Python package for managing large, multidimensional datasets (such as images or scientific data). Tiled provides an efficient way to access and process data in chunks, optimizing memory usage and performance.
