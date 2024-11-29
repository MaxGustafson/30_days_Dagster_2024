from dagster import asset, ScheduleDefinition, AssetSelection, DefaultScheduleStatus, Definitions, schedule, RunRequest

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

@schedule(
    cron_schedule = "* * * * *",
    target = AssetSelection.assets("a", "b", "c")
)
def my_minute_schedule():
    return RunRequest() 

defs = Definitions(
    assets = [a, b, c],
    schedules=  [my_minute_schedule]
)