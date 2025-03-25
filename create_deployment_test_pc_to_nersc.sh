#!/bin/bash

# Export environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Create work pool if it doesn't exist already
prefect work-pool create 'pc_to_nersc_pool'

# Build and apply deployment for test_pc_to_nersc_flow
prefect deployment build ./orchestration/flows/test_pc_to_nersc.py:test_pc_to_nersc_flow -n test_pc_to_nersc -p pc_to_nersc_pool -q pc_to_nersc_queue
prefect deployment apply test_pc_to_nersc_flow-deployment.yaml

# Optionally, you can set a schedule if needed
# prefect deployment set-schedule test_pc_to_nersc_flow/test_pc_to_nersc --cron "0 0 * * *"

echo "Deployment created successfully!"