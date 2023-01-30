from prefect.deployments import Deployment

Deployment(
    name="832",
    flow="flows/new_file_832.py",
    tags=["orchestration", "832"],
    parameters={"file_path": "test"},
)
