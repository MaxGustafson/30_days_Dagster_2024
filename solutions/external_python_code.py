import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes


with open_dagster_pipes():

    #Get Dagster-context
    context = PipesContext.get()

    fields = ['Position No', 'Event No', 'Event', 'Entity','Portfolio', 'Security','ISIN Code', 'Settl, Date', 'Settl, Amt (Cur)', 'Tot,Int (Z,Cur)', 'Currency']
    df = pd.read_csv("solutions/data/Cashflow_Interest.csv", sep=';', usecols=fields)
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

    # # send some logs to Dagster
    context.log.info(f"Parsed a csv file now have {nrow} rows")

    # report asset materialization metadata to Dagster
    context.report_asset_materialization({
        "rows" : nrow,
        "head" : str(df_cleansed.head().to_markdown()),
        "columns": str(df_cleansed.columns)
    })

    df_cleansed.to_csv('solutions/data/Cashflow_Interest_cleaned.csv')



       