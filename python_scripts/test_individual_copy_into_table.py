import snowflake.connector

SNW_ACCOUNT = 'civicapartner.west-europe.azure'
SNW_USER = 'ALUMNO4'
SNW_PASSWORD = 'Cortizo1_'
SNW_WRH = 'WH_CURSO_DATA_ENGINEERING'
SNW_DB = 'ALUMNO4_DEV_BRONZE_DB'
SNW_SCHEMA = 'SQL_SERVER_DBO'
SNW_ROLE = 'CURSO_DATA_ENGINEERING_2023'
SNW_CHANGING_TABLES = ['events', 'addresses','users', 'orders', 'order_items']

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

    new_file_name = DATA_TABLES_PATH + 'users_new_records.csv'
    new_data = 'users.csv'

    cursor.execute(
        "PUT file:/" + new_file_name.split(':', 2)[1].replace('\\', '//') + " @%" + new_data.replace('.csv', ''))
    cursor.execute("COPY INTO " + new_data.replace('.csv', '') + " FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)")

    print('fin')