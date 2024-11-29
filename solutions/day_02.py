from dagster import asset, ScheduleDefinition, AssetSelection, DefaultScheduleStatus, Definitions

@asset(group_name="my_group")
def a(): ...

@asset(
    group_name="my_group",
    deps = [a]
)
def b(): ...

@asset(
    group_name="my_group",  
    deps = [b]
)
def c(): ...

minute_schedule = ScheduleDefinition(
    name = "minute_schedule",
    target = AssetSelection.groups("my_group"),
    cron_schedule= "* * * * *",
    default_status=DefaultScheduleStatus.RUNNING
)

defs = Definitions(
    assets = [a, b, c],
    schedules=  [minute_schedule]
)