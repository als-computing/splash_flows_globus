export $(grep -v '^#' .env | xargs)

# prefect deployment build ./orchestration/flows/bl7012/move.py:process_new_file_ptycho4 -n 'process_newdata7012_ptycho4' -q bl7012_ptycho4
# prefect deployment apply process_new_file_ptycho4-deployment.yaml

prefect deployment build ./orchestration/flows/bl7012/move_recon.py:transfer_auto_recon -n 'transfer_auto_recon' -q transfer_auto_recon
prefect deployment apply transfer_auto_recon-deployment.yaml

