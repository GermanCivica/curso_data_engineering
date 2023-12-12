import create_new_records
import pandas as pd
import snowflake.connector
import os

SNW_ACCOUNT = 'civicapartner.west-europe.azure'
SNW_USER = 'ALUMNO4'
SNW_PASSWORD = 'Cortizo1_'
SNW_WRH = 'WH_CURSO_DATA_ENGINEERING'
SNW_DB = 'ALUMNO4_DEV_BRONZE_DB'
SNW_SCHEMA = 'SQL_SERVER_DBO'
SNW_ROLE = 'CURSO_DATA_ENGINEERING_2023'
SNW_CHANGING_TABLES = ['addresses', 'users', 'events', 'order_items', 'orders']

DATA_TABLES_PATH = 'C:\\Users\\germa\\Desktop\\Proyecto_final\\python_scripts\\data_tables\\'

def update_table(table, new_data):
    table_data = pd.read_csv(DATA_TABLES_PATH + table + '.csv')
    updated_data = pd.concat([table_data, new_data]).reset_index(drop=True)
    updated_data.to_csv(DATA_TABLES_PATH + table + '.csv', index=False)

def get_snowfake_tables(snowflake_cursor):
    for snowflake_table in SNW_CHANGING_TABLES:
        query_out = snowflake_cursor.execute("select * from " + snowflake_table)
        file_name = DATA_TABLES_PATH + snowflake_table + '_snowflake.csv'
        query_out.fetch_pandas_all().to_csv(file_name, index=False)

def upload_new_records(snowflake_cursor, old_data, new_data):
    # Find differences between snowflake table and internal table
    df_snowflake = pd.read_csv(DATA_TABLES_PATH + old_data)
    df_actual = pd.read_csv(DATA_TABLES_PATH + new_data)
    new_file_name = DATA_TABLES_PATH + new_data.replace('.csv', '_new_records.csv')

    id_column = df_snowflake.columns[0]

    df_new_records = df_actual[~df_actual.isin(df_snowflake)].dropna(how='all')
    df_new_records = df_new_records.dropna(axis=0, subset=[id_column])

    # Upload new data to an internal stage (if there's any)
    if not df_new_records.empty:
        df_new_records.to_csv(new_file_name, index=False)
        snowflake_cursor.execute("PUT file:/" + new_file_name.split(':', 2)[1].replace('\\', '//') + " @%" + new_data.replace('.csv', ''))
        snowflake_cursor.execute("COPY INTO " + new_data.replace('.csv', '') + " FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE")
        os.remove(new_file_name)

if __name__ == "__main__":

    # Generate new records for the selected tables
    new_address = create_new_records.new_records_addresses()
    update_table('addresses', new_address)

    new_user = create_new_records.new_records_user(new_address)
    update_table('users', new_user)

    new_events = create_new_records.new_records_events(new_user)
    update_table('events', new_events)

    new_order_items = create_new_records.new_records_order_items(new_events)
    update_table('order_items', new_order_items)

    new_orders = create_new_records.new_records_orders(new_events, new_user, new_order_items)
    update_table('orders', new_orders)

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

    # Store the data tables in files, so we can add new records
    get_snowfake_tables(cursor)

    # Get the new records and upload them to Snowflake
    data_tables = next(os.walk(DATA_TABLES_PATH), (None, None, []))[2]
    for table in SNW_CHANGING_TABLES:
        updated_table_file = table + '.csv'
        if updated_table_file in data_tables:
            snowflake_table_file = table + '_snowflake.csv'
            upload_new_records(cursor, snowflake_table_file, updated_table_file)