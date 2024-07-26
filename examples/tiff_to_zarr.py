#!/usr/bin/env python

import os
import argparse
import zarr
from ngff_zarr import (
    detect_cli_io_backend,
    cli_input_to_ngff_image,
    to_multiscales,
    to_ngff_zarr,
    Methods,
    config
)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Convert TIFF files to NGFF Zarr format.')
    parser.add_argument('tiff_directory', type=str, help='Directory containing the TIFF files.')
    parser.add_argument('--zarr_directory', type=str, default=None, help='Directory to store the Zarr output. Defaults to a new folder in the input directory.')
    return parser.parse_args()

def set_permissions_recursive(path, permissions=0o2775):
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            os.chmod(os.path.join(root, dir), permissions)
        for file in files:
            os.chmod(os.path.join(root, file), permissions)
    os.chmod(path, permissions)  # Also set permissions for the top-level directory

def main():
    args = parse_arguments()

    tiff_dir = args.tiff_directory
    zarr_dir = args.zarr_directory 

    if not os.path.isdir(tiff_dir):
        raise TypeError("The specified TIFF directory is not a valid directory")

    file_names = os.listdir(tiff_dir)
    file_paths = [os.path.join(tiff_dir, file_name) for file_name in file_names if not file_name.startswith('.')]
    file_paths.sort()

    if not file_paths:
        raise ValueError("No TIFF files found in the specified directory")
    if not zarr_dir:
        last_part = os.path.basename(os.path.normpath(tiff_dir))
        zarr_dir = os.path.abspath(os.path.join(tiff_dir, '..', last_part + '.zarr'))
        # zarr_dir = os.path.abspath(os.path.join(tiff_dir, '..', os.path.basename(tiff_dir) + '.zarr'))
    if not os.path.exists(zarr_dir):
        os.makedirs(zarr_dir)

    print('Output directory: ' + zarr_dir)

    # Build NGFF Zarr directory
    # config.cache_store = zarr.storage.DirectoryStore("/alsuser/pscratch/zarr-ngff-cache", dimension_separator="/")
    backend = detect_cli_io_backend(file_paths)
    image = cli_input_to_ngff_image(backend, file_paths)
    # The scale and axis units are the same as the one printed in the reconstruction script
    image.scale = {'z': 0.65, 'y': 0.65, 'x': 0.65}
    image.axes_units = {'z': 'micrometer', 'y': 'micrometer', 'x': 'micrometer'}
    multiscales = to_multiscales(image, method=Methods.DASK_IMAGE_GAUSSIAN)
    to_ngff_zarr(zarr_dir, multiscales)
    print('NGFF Zarr created')

    # Set permissions for the output directory and its contents
    set_permissions_recursive(zarr_dir)


if __name__ == "__main__":
    main()