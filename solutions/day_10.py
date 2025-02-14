from dagster import asset, RetryPolicy, AssetExecutionContext, MetadataValue, sensor, SensorEvaluationContext, RunRequest, SkipReason, Definitions, Config, ConfigurableResource
import pandas as pd
import duckdb
import os, time
from pydantic import Field
from typing import Optional
import numpy as np

class my_file_path(Config):
    file_path : str = Field(default="solutions/data/Cashflow_Interest.csv", description="Default csv_name")

class my_csv_handler(ConfigurableResource):
    #validate_assignement = True

    path_src : str
    path_trg : Optional[str] = None
    sep : str 
    usecols : Optional[list] = None 
    my_converters : Optional[dict] = None 

    def write_csv(self, df : pd.DataFrame):
        if self.path_trg is None:
            return  
        df.to_csv(self.path_trg)

    def read_csv(self):

        if self.my_converters is None:
            return pd.read_csv(self.path_src, sep = self.sep, usecols = self.usecols)
        
        if self.my_converters == {}:
            return pd.read_csv(self.path_src, sep = self.sep, usecols = self.usecols, converters = {'Settl_Amt_Cur':float,'Tot_Int_Z_Cur':float})

        raise Exception('Wrong my_converters parameter passed')

@asset(
    retry_policy=RetryPolicy(max_retries=2),
    owners = ["owner.ownersson@gmail.se"],
    tags = {'layer' : 'bronze'},
    kinds = {'pandas', 'csv'}
)
def cashflow_raw(context: AssetExecutionContext, config : my_file_path, csv_resource_raw : my_csv_handler):
    """ Reads in Raw data from Cashflow_Interest.""" 

    df = csv_resource_raw.read_csv()
    df_cleansed = df.drop(range(len(df)-16,len(df)))
    renaming = {
        'Position No':'Position_No',
        'Event No':'Event_No',
        'ISIN Code':'ISIN_Code', 
        'Settl, Date':'Settl_Date',
        'Settl, Amt (Cur)' : 'Settl_Amt_Cur',
        'Tot,Int (Z,Cur)' : 'Tot_Int_Z_Cur'
    }
    df_cleansed.rename(columns = renaming, inplace=1)

    df_cleansed['Settl_Amt_Cur'] = df_cleansed['Settl_Amt_Cur'].apply(lambda x: x.replace(' ','').replace(',','.'))
    df_cleansed['Tot_Int_Z_Cur'] = df_cleansed['Tot_Int_Z_Cur'].apply(lambda x: x.replace(' ','').replace(',','.'))
    df.columns.str.strip()

    nrow = len(df_cleansed)
    context.log.info(f"Parsed a csv file now have {nrow} rows")
    context.add_output_metadata({
        "rows" : nrow,
        "head" : MetadataValue.md(df_cleansed.head().to_markdown()),
        "columns":MetadataValue.md(str(df_cleansed.columns))
    })

    csv_resource_raw.write_csv(df_cleansed)

@asset(
    deps = [cashflow_raw],
    kinds = {'pandas', 'csv'}
)
def cashflow_summary(context: AssetExecutionContext, csv_resource_cleansed : my_csv_handler): 
    cashflow_cleansed : pd.DataFrame = csv_resource_cleansed.read_csv()
    cashflow_cleansed.columns.str.strip()
    context.log.info(cashflow_cleansed.head)

    context.add_output_metadata({
        "rows" : len(cashflow_cleansed),
        "head" : MetadataValue.md(cashflow_cleansed.head().to_markdown()),
        "columns":MetadataValue.md(str(cashflow_cleansed.columns))
    })

    df_cashflow_summary = cashflow_cleansed.groupby('Security')['Settl_Amt_Cur'].mean()
    context.log.info(cashflow_summary)
    return df_cashflow_summary

@asset(
    deps = [cashflow_raw],
    kinds = {'pandas', 'csv','duckdb'}
)
def cashflow_duckdb(): 
    with duckdb.connect("solutions/data/cashflow_duck.db") as con:
        con.sql("DROP TABLE IF EXISTS cashflow_interest_cleaned ")
        con.sql("CREATE TABLE cashflow_interest_cleaned AS SELECT * FROM 'solutions/data/Cashflow_Interest_cleaned.csv'")

@asset(
    deps = [cashflow_summary]
)
def c(): ...

@sensor(
    target = ['cashflow_raw*']
) 
def run_pipeline_on_csv_change(context : SensorEvaluationContext):
    file_path = 'solutions/data/Cashflow_Interest.csv'

    last_modified_time = os.path.getmtime(file_path)

    if context.cursor is None:
        context.log.info({f'Launching initial run.'})
        context.update_cursor(str(last_modified_time))
        return RunRequest()
    
    if last_modified_time > float(context.cursor):
        context.log.info(f'Lanching run because last lanch was {context.cursor} and file updated {str(last_modified_time)}')
        context.update_cursor(str(last_modified_time))
        return RunRequest()
    
    return SkipReason(f'The file was last updated at {context.cursor} and we observed the last update in file {last_modified_time}')

defs = Definitions(
    assets = [cashflow_raw, cashflow_duckdb, cashflow_summary, c],
    sensors = [run_pipeline_on_csv_change],
    resources = {
        "csv_resource_raw" : my_csv_handler(path_src = 'solutions/data/Cashflow_Interest.csv', path_trg = 'solutions/data/Cashflow_Interest_cleaned.csv', sep = ';', usecols = ['Position No', 'Event No', 'Event', 'Entity','Portfolio', 'Security','ISIN Code', 'Settl, Date', 'Settl, Amt (Cur)', 'Tot,Int (Z,Cur)', 'Currency']),
        "csv_resource_cleansed" : my_csv_handler(path_src = 'solutions/data/Cashflow_Interest_cleaned.csv', sep = ',', my_converters = {})
    }
)
    

