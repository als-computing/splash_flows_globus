# Installation

Splash Flows Globus contains configuration, code and infrastructure for moving data and running automated computing tasks when X-ray scans are captured at the Advanced Light Source.

This package is primarily written in Python, and relies on additional services such as Prefect (an orchestration tool), Docker (for containers), and Globus (for Transfer and Compute).

These flows can be run from the command line, or built into a self-contained docker container, which can be found under [Releases](https://github.com/als-computing/splash_flows_globus/releases). 

---

## Getting started

### Clone this repo

```
$   git clone git@github.com:als-computing/splash_flows_globus.git
$   cd splash_flows_globus
```

It is recommended to create a new conda environment when pulling the source code locally, so you do not end up with conflicts with your global environment. Conda is an OS-agnostic, system-level binary package and environment manager, and installation information can be found [here](https://anaconda.org/anaconda/conda).

Once you have conda on your system, you can create and activate a new environment like this:

```
conda create -n splash_flows_globus_env python=3.11.9
conda activate splash_flows_globus_env
```

Now that you have activated your environment, you can install the necessary Python dependencies by calling the following line in the root directory of the `splash_flows_globus` project:

```
pip install -r reqirements.txt
```

Now that the requirements are installed, you can install the package:

```
pip install -e .
```

---
