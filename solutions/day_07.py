from dagster import asset, RetryPolicy, AssetExecutionContext, MetadataValue
import pandas as pd
import duckdb

@asset(
    retry_policy=RetryPolicy(max_retries=2),
    owners = ["owner.ownersson@gmail.se"],
    tags = {'layer' : 'bronze'},
    kinds = {'pandas', 'csv'}
)
def cashflow_raw(context: AssetExecutionContext):
    """ Reads in Raw data from Cashflow_Interest.""" 

    fields = ['Position No', 'Event No', 'Event', 'Entity','Portfolio', 'Security','ISIN Code', 'Settl, Date', 'Settl, Amt (Cur)', 'Tot,Int (Z,Cur)', 'Currency']
    df = pd.read_csv('solutions/data/Cashflow_Interest.csv', sep=';', usecols=fields)
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

    nrow = len(df_cleansed)
    context.log.info(f"Parsed a csv file now have {nrow} rows")
    context.add_output_metadata({
        "rows" : nrow,
        "head" : MetadataValue.md(df_cleansed.head().to_markdown()),
        "columns":MetadataValue.md(str(df_cleansed.columns))
    })

    df_cleansed.to_csv('solutions/data/Cashflow_Interest_cleaned.csv')

@asset(
    deps = [cashflow_raw]
)
def cashflow_summary(context: AssetExecutionContext): 
    converters = {'Settl, Amt (Cur)':float,'Tot,Int (Z,Cur)':float}
    cashflow_raw : pd.DataFrame = pd.read_csv('solutions/data/Cashflow_Interest_cleaned.csv', converters = {'Settl, Amt (Cur)':float})
    df_cashflow_summary = cashflow_raw.groupby('Security')['Settl_Amt_Cur'].mean()
    context.log.info(cashflow_summary)
    return df_cashflow_summary

@asset(
    deps = [cashflow_summary],
    kinds = {'pandas', 'csv','duckdb'}
)
def cashflow_duckdb(): 
    with duckdb.connect("solutions/data/cashflow_duck.db") as con:
        con.sql("CREATE TABLE cashflow_interest_cleaned AS SELECT * FROM 'solutions/data/Cashflow_Interest_cleaned.csv'")

        

@asset(
    deps = [cashflow_summary]
)
def c(): ...


