export $(grep -v '^#' .env | xargs)


# create 'alfc_flow_pool'
prefect work-pool create 'alcf_flow_pool'
# create 'aclf_prune_pool'
prefect work-pool create 'alcf_prune_pool'


prefect deployment build ./orchestration/flows/bl832/alcf.py:alcf_recon_flow -n alcf_recon_flow -q bl832 -p alcf_flow_pool
prefect deployment apply alcf_recon_flow-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_raw -n prune_alcf832_raw -q bl832 -p alcf_prune_pool
prefect deployment apply prune_alcf832_raw-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_scratch -n prune_alcf832_scratch -q bl832 -p alcf_prune_pool
prefect deployment apply prune_alcf832_scratch-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832_raw -n prune_data832_raw -q bl832 -p alcf_prune_pool
prefect deployment apply prune_data832_raw-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832_scratch -n prune_data832_scratch -q bl832 -p alcf_prune_pool
prefect deployment apply prune_data832_scratch-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_nersc832_alsdev_scratch -n prune_nersc832_alsdev_scratch -q bl832 -p alcf_prune_pool
prefect deployment apply prune_nersc832_alsdev_scratch-deployment.yaml