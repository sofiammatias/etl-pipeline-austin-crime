from prefect import task, flow
from extract_json_to_postgres import write_json_to_postgres_main
from load_dfs_to_postgres import create_dfs_to_postgres_main

@task
def extract_json_to_postgres_task():
    return write_json_to_postgres_main()
@task
def load_dfs_to_postgres_task():
    return create_dfs_to_postgres_main()

@flow
def etl_workflow():
    extract_json_to_postgres_task()
    load_dfs_to_postgres_task()

etl_workflow()

if __name__ == "__main__":
    etl_workflow.serve(name="etl-workflow",
                      tags=["projects"],
                      interval=900)