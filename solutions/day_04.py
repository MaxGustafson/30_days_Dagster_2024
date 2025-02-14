from dagster import asset, RetryPolicy
import random as rand

@asset(
    retry_policy=RetryPolicy(max_retries=4)
)
def a(): 
    if(rand.randint(0,3) >= 1):
        raise Exception()
    else:
        return

@asset(
    deps = [a],
    retry_policy=RetryPolicy(max_retries = 2, delay=5)
)
def b():
    if(rand.randint(0,9) >= 1):
        raise Exception()
    else:
        return

@asset(
    deps = [b]
)
def c(): ...


