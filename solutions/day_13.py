from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition


days = DailyPartitionsDefinition(start_date = '2025-01-01')

@asset(
    partitions_def=days
)
def a(context : AssetExecutionContext):
    partition = context.partition_key
    context.log.info(partition)
    return

@asset(
    deps = [a]
)
def b(): ...

@asset(
    deps = [b]
)
def c(): ...