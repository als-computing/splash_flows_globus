export $(grep -v '^#' .env | xargs)

# create 'nersc_flow_pool'
prefect work-pool create 'nersc_flow_pool'

# nersc_flow_pool
    # in docker-compose.yaml:
    # command: prefect agent start --pool "nersc_flow_pool"
prefect deployment build ./orchestration/flows/bl832/nersc.py:nersc_recon_flow -n nersc_recon_flow -p nersc_flow_pool -q nersc_recon_flow_queue
prefect deployment apply nersc_recon_flow-deployment.yaml
