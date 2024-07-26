export $(grep -v '^#' .env | xargs)


# prefect deployment build ./orchestration/flows/bl832/alcf.py:process_new_832_ALCF_flow -n process_new_832_ALCF_flow -q bl832
# prefect deployment apply process_new_832_ALCF_flow-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_raw -n prune_alcf832_raw -q bl832 -p test_pool
prefect deployment apply prune_alcf832_raw-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_alcf832_scratch -n prune_alcf832_scratch -q bl832 -p test_pool
prefect deployment apply prune_alcf832_scratch-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_nersc832_alsdev_scratch -n prune_nersc832_alsdev_scratch -q bl832 -p test_pool
prefect deployment apply prune_nersc832_alsdev_scratch-deployment.yaml