import collections
import builtins
import os

import yaml


def read_config(config_file="globus_endpoints.yml"):
    with open(config_file, "r") as end_file:
        globus_config = yaml.safe_load(end_file)
    return expand_environment_variables(globus_config)


def expand_environment_variables(config):
    """Expand environment variables in a nested config dictionary
    VENDORED FROM dask.config and tiled.
    This function will recursively search through any nested dictionaries
    and/or lists.
    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables
    Returns
    -------
    config : same type as input
    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}
    """
    if isinstance(config, collections.abc.Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config
