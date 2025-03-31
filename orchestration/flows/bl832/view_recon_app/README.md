# Tomography Reconstruction Visualizer

## (Powered by itk-vtk-viewer)

This web application allows users to load their reconstructed volumes using Tiled Browser (in development) into the itk-vtk-viewer widget, all within the same web page.

TODO:
- Add a 'listener' mode that loads in the most recent tomographic reconstruction once processing is completed and the data is available.
- Containerize the application to make it easily installable
    - Are there deployments we want to support? Could we use this at other light sources?


-----------------------
# Installation 

## `Bluesky Tiled`

Host your reconstructed data using `Tiled`, which we use to connect data servers to front end applications such as this one.

#### Prepare environment

`Tiled` support for `Zarr` is a work in progress, but there is a specific branch you can use for this application:
- `add-zarr-forked` branch: https://github.com/davramov/tiled/tree/add-zarr-forked
- This was a small addition to this PR on the source repo: https://github.com/bluesky/tiled/pull/774

To install this version of Tiled, I recommend creating a new Conda environment and following the "[Install Tiled from Source](https://blueskyproject.io/tiled/tutorials/installation.html#source)" instructions:

```bash
conda create env -n "tiled_zarr_env python=3.12" 
conda activate tiled_zarr_env
```

#### Clone and install repository
Instead of installing the main version, use this fork with Zarr support:
```bash
git clone -b add-zarr-forked https://github.com/davramov/tiled.git`
cd tiled
pip install -e ".[all]"
```

#### Start Tiled

Open a new terminal, and either navigate to the directory containing your zarr projects, or specify the full path directly:

```bash
cd [go/to/your/zarr/projects/]
TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:5174 http://localhost:8082" tiled serve directory "../../../data/tomo/scratch/" --public --verbose
```

If that is successful, you should see something like this:
```bash
(tiled_zarr_env) you@your-computer tiled_zarr % TILED_ALLOW_ORIGINS="http://localhost:3000 http://localhost:5174 http://localhost:8082" tiled serve directory "../../../data/tomo/scratch/" --public --verbose
Creating catalog database at /var/folders/7t/17b_zxx55jnggw80_6672tbh0000gn/T/tmp94wbqw7y/catalog.db

    Tiled server is running in "public" mode, permitting open, anonymous access
    for reading. Any data that is not specifically controlled with an access
    policy will be visible to anyone who can connect to this server.


    Navigate a web browser or connect a Tiled client to:

    http://127.0.0.1:8000?api_key=ee7caca056af09c62993ffa789689bf181d41a4e885544c070e68b32339ed1c0


    Because this server is public, the '?api_key=...' portion of
    the URL is needed only for _writing_ data (if applicable).


[-] INFO:     Started server process [15895]
[-] INFO:     Waiting for application startup.
Tiled version 0.1.dev2517+g202f10e
[-] INFO:     Application startup complete.
[-] INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
[58c42c2098d320fd] 127.0.0.1:61412 (unset) - "GET /api/v1/ HTTP/1.1" 200 OK
[74cb5a4350af0bb9] 127.0.0.1:61412 (unset) - "GET /api/v1/metadata/?include_data_sources=false HTTP/1.1" 200 OK
Server is up. Indexing files in ../../../data/tomo/scratch/...
  Overwriting '/'
```

The important thing to grab here is the URL with the api_key: 

<u>**`http://127.0.0.1:8000?api_key=ee7caca056af09c62993ffa789689bf181d41a4e885544c070e68b32339ed1c0`**</u>

In your web browser, open the URL you see in your terminal to activate the Tiled session. The Zarr files that are indexed here and viewable in the main Tiled UI are loadable from the Tiled Browser widget in the React App.


## `itk-vtk-viewer`

#### Install

[Official Documentation](https://kitware.github.io/itk-vtk-viewer/docs/cli.html)

[Install or update Node.js](https://nodejs.org/en/download)

Then, install `itk-vtk-viewer`

**npm version**
```bash
npm install itk-vtk-viewer -g
```

**specific version**
The `itk-vtk-viewer` interface is customizable, so you can install a different version following these steps:

```bash

```

#### Run

Once you have a version of `itk-vtk-viewer` installed in your environment, you can start it in the command line with the following command:

```bash
itk-vtk-viewer --port 8082
```

We specify `--port 8082`, as this is what the React App is configured to listen to by default.


### Start the `React` App

Run in your terminal

```bash
cd /orchestration/flows/bl832/view_recon_app/
npm run dev
```

```
  VITE v6.0.11  ready in 139 ms

  ➜  Local:   http://localhost:5174/
  ➜  Network: use --host to expose
  ➜  press h + enter to show help
```

Navigate to http://localhost:5174/ in your web browser

