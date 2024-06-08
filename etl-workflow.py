from prefect import task, flow, get_run_logger
from extract_json_to_postgres import write_json_to_postgres_main
from load_dfs_to_postgres import create_dfs_to_postgres_main


@task
def extract_json_to_postgres_task():
    logger = get_run_logger()
    logger_msg1, logger_msg2 = write_json_to_postgres_main()
    logger.info(logger_msg1)
    logger.info(logger_msg2)



@task
def load_dfs_to_postgres_task():
    logger = get_run_logger()
    logger_msg = create_dfs_to_postgres_main()
    logger.info(logger_msg)

@flow
def etl_workflow():
    logger = get_run_logger()
    extract_json_to_postgres_task()
    load_dfs_to_postgres_task()
    logger.info ("Finished creating final transformed tables")


if __name__ == "__main__":
    etl_workflow()