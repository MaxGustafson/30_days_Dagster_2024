from dagster import asset, ScheduleDefinition, AssetSelection, DefaultScheduleStatus, Definitions, schedule, RunRequest, sensor, AutomationCondition

@asset(group_name="my_group",
       automation_condition=AutomationCondition.on_cron('* * * * *'))
def a(): ...

@asset(
    group_name="my_group",
    automation_condition= AutomationCondition.any_downstream_conditions(),
    deps = [a]
)
def b(): ...

@asset(
    group_name="my_group",  
    deps = [b],
    automation_condition=AutomationCondition.on_cron('*/2 * * * *')
)
def c(): ...


defs = Definitions(
    assets = [a, b, c]
)
