export $(grep -v '^#' .env | xargs)

prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file_ptycho4 -n 'process_newdata7012_ptycho4' -q bl7012_ptycho4
prefect deployment apply process_new_file_ptycho4-deployment.yaml

prefect deployment build ./orchestration/flows/bl7012/move_recon.py:transfer_auto_recon -n 'transfer_auto_recon' -q transfer_auto_recon
prefect deployment apply transfer_auto_recon-deployment.yaml


<<<<<<< HEAD
=======

prefect deployment build ./orchestration/flows/bl832/prune.py:prune_spot832 -n prune_spot832 -q bl832
prefect deployment apply prune_spot832-deployment.yaml


prefect deployment build ./orchestration/flows/bl832/prune.py:prune_data832 -n 'prune_data832' -q bl832
prefect deployment apply prune_data832-deployment.yaml

prefect deployment build ./orchestration/flows/bl7012/move.py:test_transfers_7012 -n 'test_data7012' -q bl7012
prefect deployment apply test_transfers_7012-deployment.yaml

prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file -n 'process_newdata_7012' -q bl7012
prefect deployment apply process_new_file-deployment.yaml
>>>>>>> 369f16cf4e4e4a08593a033a7ed5299245f13277
