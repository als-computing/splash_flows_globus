export $(grep -v '^#' .env | xargs)


prefect deployment build ./orchestration/flows/bl832/move.py:process_new_832_file -n new_file_832 -q bl832
prefect deployment apply process_new_832_file-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/move.py:test_transfers_832 -n test_transfers_832 -q bl832
prefect deployment apply test_transfers_832-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_spot832 -n prune_spot832 -q bl832
prefect deployment apply prune_spot832-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832 -n 'prune_data832' -q bl832
prefect deployment apply prune_data832-deployment.yaml

