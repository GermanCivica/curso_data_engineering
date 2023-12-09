import pandas as pd
import requests
import snowflake.connector

SNW_ACCOUNT = 'civicapartner.west-europe.azure'
SNW_USER = 'ALUMNO4'
SNW_PASSWORD = 'Cortizo1_'
SNW_WRH = 'WH_CURSO_DATA_ENGINEERING'
SNW_DB = 'ALUMNO4_DEV_BRONZE_DB'
SNW_SCHEMA = 'SQL_SERVER_DBO'
SNW_ROLE = 'CURSO_DATA_ENGINEERING_2023'

DATA_TABLES_PATH = 'C:\\Users\\germa\\Desktop\\Proyecto_final\\python_scripts\\data_tables\\'

if __name__ == "__main__":

    # Connect to Snowflake to retrieve the appropiate data tables
    con = snowflake.connector.connect(
        user=SNW_USER,
        password=SNW_PASSWORD,
        account=SNW_ACCOUNT,
        warehouse=SNW_WRH,
        database=SNW_DB,
        schema=SNW_SCHEMA
    )

    cursor = con.cursor()
    query_out = cursor.execute("select * from addresses")

    # Store the data tables in files so we can add new registers
    #file_name = DATA_TABLES_PATH + 'users.csv'
    file_name = DATA_TABLES_PATH + 'addresses_snowflake.csv'
    query_out.fetch_pandas_all().to_csv(file_name, index=False)

    # Find differences between snowflake table and internal table
    df_snowflake = pd.read_csv(file_name)
    df_actual = pd.read_csv(file_name.replace('_snowflake.csv', '.csv'))

    new_file_name = file_name.replace('_snowflake.csv', '_new_records.csv')
    df_new_records = df_actual[~df_actual.isin(df_snowflake)].dropna(how='all')
    df_new_records.to_csv(new_file_name, index=False)

    # Upload new data to an internal stage
    # con.cursor().execute("PUT file:/" + file_name.split(':', 2)[1].replace('\\', '//') + " @%users")
    con.cursor().execute("PUT file:/" + new_file_name.split(':', 2)[1].replace('\\', '//') + " @%addresses")
    con.cursor().execute("COPY INTO addresses FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)")