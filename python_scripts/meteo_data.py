import os
import pandas as pd
import requests
import json
import snowflake.connector

SNW_ACCOUNT = 'civicapartner.west-europe.azure'
SNW_USER = 'ALUMNO4'
SNW_PASSWORD = '*****'
SNW_WRH = 'WH_CURSO_DATA_ENGINEERING'
SNW_DB = 'ALUMNO4_DEV_BRONZE_DB'
SNW_SCHEMA = 'SQL_SERVER_DBO'
SNW_ROLE = 'CURSO_DATA_ENGINEERING_2023'

DATA_TABLES_PATH = 'C:\\Users\\germa\\Desktop\\Proyecto_final\\python_scripts\\data_tables\\'

METEO_API_KEY = '******************'

def get_meteo_data(city: str, dates: list[str]) -> pd.DataFrame:
    start_dates = pd.date_range(dates[0], dates[-1], freq ='MS').strftime("%Y-%m-%d").tolist()
    end_dates = pd.date_range(dates[0], dates[-1], freq ='M').strftime("%Y-%m-%d").tolist()
    final_dates = list(map(lambda x,y: x+'/'+y, start_dates, end_dates))

    meteo_data_city = pd.DataFrame()

    for date in final_dates:
        url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"\
               + city.lower() + "/" + date + "?unitGroup=metric&key=" + METEO_API_KEY + "&contentType=json"
        response = requests.get(url)
        response_json = response.json()
        city_data = pd.DataFrame([[response_json['address'], response_json['latitude'], response_json['longitude']]], columns=['city', 'latitude', 'longitude'])
        days_data = pd.DataFrame(response_json.days)
        clean_days_data = pd.DataFrame()
        for day in days_data:
            days_data_short = {x: day[x] for x in day if x not in ['preciptype', 'hours', 'stations']}
            clean_days_data = pd.concat([clean_days_data, days_data_short])

        city_data_month = pd.concat([city_data, clean_days_data], axis=1)
        city_data_month.fillna({'city': city_data_month.iat[0, 0], 'latitude': city_data_month.iat[0, 1], 'longitude': city_data_month.iat[0, 2]})
        meteo_data_city = pd.concat([meteo_data_city, city_data_month])

    return meteo_data_city



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

    dates = ['2021-01-01', '2021-03-31']
    city = 'Chicago'

    jan_march_chicago = get_meteo_data(city, dates)

    jan_march_chicago
