export $(grep -v '^#' .env | xargs)


# Create work pools. If a work pool already exists, it will throw a warning but that's no problem
prefect work-pool create 'new_file_832_flow_pool'


prefect deployment build ./orchestration/flows/bl832/move.py:test_transfers_832_grafana -n test_transfers_832_grafana -p new_file_832_flow_pool -q test_transfers_832_queue
prefect deployment apply test_transfers_832_grafana-deployment.yaml

