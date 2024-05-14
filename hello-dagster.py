from dagster import (AssetIn, asset, AssetExecutionContext,
job, multiprocess_executor, Definitions, define_asset_job, AssetSelection, ScheduleDefinition)
import time


@asset(compute_kind='python',group_name="get_started")
def my_zero_asset(context: AssetExecutionContext):
    """
    Zero dagster asset

    """
    time.sleep(2)
    l = [0,0,0]
    print(f"This is a print message with the list's values {l}")
    context.log.info(f"This is a log message with the list's values {l}")
    return l

@asset(compute_kind='python',group_name="get_started")
def my_first_asset(context: AssetExecutionContext):
    """
    First dagster asset

    """
    time.sleep(1)
    l = [1, 2, 3]
    print(f"This is a print message with the list's values {l}")
    context.log.info(f"This is a log message with the list's values {l}")
    return l


@asset(ins={"upstream": AssetIn(key="my_first_asset")}, key="my_awesome_second_asset", compute_kind='python',group_name="get_started")
def my_second_asset(context: AssetExecutionContext, upstream: list):
    """
    Second dagster asset

    """
    l = upstream + [4, 5, 6]
    print(f"This is a print message with the list's values {l}")
    context.log.info(f"This is a log message with the list's values {l}")
    return l

@asset(ins={
    "first_upstream": AssetIn(key="my_first_asset"),
    "second_upstream": AssetIn(key="my_awesome_second_asset")
    }, compute_kind='duckdb',group_name="get_started")
def my_third_asset(context: AssetExecutionContext, first_upstream: list, second_upstream):
    """
    Second dagster asset

    """
    l = {
        "first_asset": first_upstream,
        "second_asset": second_upstream,
        "third_asset": second_upstream + [7, 8, 9]
    }
    print(f"This is a print message with the list's values {l}")
    context.log.info(f"This is a log message with the list's values {l}")
    return l

defs = Definitions(
    assets=[my_zero_asset,my_first_asset, my_second_asset, my_third_asset],
    jobs=[
        define_asset_job(
            name="hello_dagster_job",
            # selection=[my_first_asset, my_second_asset, ],
            selection=AssetSelection.groups("get_started"),
            # executor_def=multiprocess_executor.configured({"max_concurrent": 4}),
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="hello_dagster_schedule",
            job_name="hello_dagster_job",
            cron_schedule="* * * * *"
        )
    ])


# @job(executor_def=multiprocess_executor.configured({"max_concurrent": 4}))
# def my_parallel_job():
#     my_first_asset()
#     my_zero_asset()


# @asset()  # option 2 dependency -  uses the data from the first asset
# def my_second_asset(context: AssetExecutionContext, my_first_asset: list):
#     """
#     Second dagster asset

#     """
#     l = my_first_asset + [4, 5, 6]
#     print(f"This is a print message with the list's values {l}")
#     context.log.info(f"This is a log message with the list's values {l}")
#     return l


# @asset(deps=[my_first_asset])  # option 1 dependency -  doesn't use the data or output from the first asset
# def my_second_asset(context: AssetExecutionContext):
#     """
#     Second dagster asset

#     """
#     l = [4, 5, 6]
#     print(f"This is a print message with the list's values {l}")
#     context.log.info(f"This is a log message with the list's values {l}")
#     return l