from dagster import asset, RetryPolicy, AssetExecutionContext, MetadataValue
import pandas as pd

@asset(
    retry_policy=RetryPolicy(max_retries=2),
    owners = ["owner.ownersson@gmail.se"],
    tags = {'layer' : 'bronze'},
    kinds = {'pandas', 'csv'}
)
def cashflow_Raw(context: AssetExecutionContext):
    """ Reads in Raw data from Cashflow_Interest. TODO: Clean up file!""" 
    df = pd.read_csv('solutions/data/Cashflow_Interest.csv', sep=';')
    nrow = len(df)
    context.log.info(f"Parsed a csv file now have {nrow} rows")
    context.add_output_metadata({
        "rows" : nrow,
        "head" : MetadataValue.md(df.head().to_markdown()),
        "columns":MetadataValue.md(str(df.columns))
    })
    return df

@asset(
    deps = [cashflow_Raw]
)
def b(context: AssetExecutionContext): ...
  

@asset(
    deps = [b]
)
def c(): ...



