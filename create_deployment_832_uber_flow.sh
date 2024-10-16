export $(grep -v '^#' .env | xargs)

prefect deployment build ./orchestration/flows/bl832/decision_flow.py:decision_flow -n run_832_decision_flow -q bl832
prefect deployment apply run_832_decision_flow.yaml