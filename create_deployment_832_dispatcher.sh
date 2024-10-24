export $(grep -v '^#' .env | xargs)

prefect deployment build ./orchestration/flows/bl832/decision_flow.py:dispatcher -n run_832_dispatcher -q bl832
prefect deployment apply dispatcher-deployment.yaml