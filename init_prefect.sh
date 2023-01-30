prefect deployment build ./orchestration/flows/bl832/move.py:process_new_832_file -n new_file_832 -q bl832 -t bl832
prefect deployment apply process_new_832_file-deployment.yaml

prefect deployment build ./orchestration/flows/bl832/move.py:test_transfers -n 832_test_transfer -q bl832 -t bl832 -t test
prefect deployment apply test_transfers-deployment.yaml