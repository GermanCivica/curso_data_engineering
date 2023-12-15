

## CÍVICA DATA ENGINEER COURSE 2023: FINAL PROJECT

**Author: Germán Artigot Cortizo**

**Overview**

In this project we start with a data set contaning all the relevant information regarding the sales of a plant e-commerce. From the events registered in the web page, the users registered and their addresses, to the available products, the amount of them acquired in each sale and the information on the order resulting from it.

The approach will consisits on, first, modelling the data to enable its correct and fast utilization for analysis and consulting. To do this we'll apply a metal-based architecture in which **bronze** is the raw, untransformed data from our source (i.e: the aforementioned data set), **silver** will be conformed by tables that are very similar to those in bronze but with small corrections (i.e: null/empty values treatment, data type casting...) and **gold** will have additional tables aggregating data from silver conforming dimensions and a fact-table as well as several data marts to be used by different departments of the enterprise to provide insights on relevant data and facilitating KPI identification. As a result, the data will have a **snowflake model**.

Once our data model is fully developed and tested, the **additional value** of this project will steam from a **python project** that aims to *"make the data set alive"*. This meaning that it will be able to **automatically create new data and upload it to the bronze tables**, the difference between this project and other ones which also use python to ingest new data in the set is that while others add new data to just one table and that is manually created by the user (limiting the executions). This python project generates new data simulating the whole buying process from a registered user while maintaining the coherence between the new records in the tables. What this enables is for this process to be automatically launched as many times as desired and it will indefinetely add new and valid data to bronze.

The **ingestion of the new data** will be handled by **python**, the **data transformation** process will be done using **dbt** and the **data warehouse** will be on **Snowflake**.

## Project

```markdown
├── proyecto\_final
│   ├── analysis
│   ├── macros											# Relevant for dbt transformations
│   ├── models
│   │   ├── marts										# Gold
│   │   ├── staging										# Silver
│   ├── python\_scripts
│   │   ├── COPIA\_DE\_SEGURIDAD\_BRONZE				# Data Sources security backup
│   │   ├── data\_tables								# Contains the data tables with new data
│   │   ├── generator									# CSV files with relevant info for new data generation
│   │   ├── create\_new\_records.py						# Script that creates the new records
│   │   ├── meteo\_data.py								# Future development
│   │   ├── test\_individual\_copy\_into\_table.py		
│   │   ├── upload\_new\_records.py						# Script that uploads the new records to Snowflake
├── seeds
├── snapshots
├── tests												# Adhoc tests created for dbt
```

### dbt

The differenciation factors of this project in dbt lie on the gold tables and tests, as well as the changes in the project needed for it to adapt to the now continue-update nature of the data set:
#### Tests
- **no\_negatives**: checks that no negative numeric values are present in the table and column specified
- **utc\_times**: as part of the transformations implied transforming each timestamp in bronze to UTC times, we apply this test to every column in silver containing a timestamp to make sure it is indeed in UTC.
#### Gold
- ##### Core
  - Intermediate
	* **int\_orders\_products**: Provides information on each product in each order regarding how many of them have been ordered historically and on the month of the order alone.
	* **int\_user\_events**: Groups all the events in the "events" table counting how many of them are for each type.
	* **int\_user\_orders**: Analysis on the purchases by user, how much have thy spent on products alone, on shipping costs and how much have they saved via promo codes.


  - **dim_shipping**: Aggregated information regarding shipments.
    
  - **fct_orders_products**:	Following a Kimball approach to teh data sect the *grain* was reduced from information on each order to information on each purchase for each individual product inside an order. This was done joining several stages such as orders (obviously), order\_items and events.
- ##### Marts
  - Finance
	* **products_budget_vs_sales**:  In-depth study on the amount purchased for each product against the monthly budget that the same product has. For each product with a budget for a month and a year, the amount of units sold and the income made by that same product is obtained, a flag value indicating whether if the sales for the product exceed the budget is also provided.
	
  - Marketing
	* **user_purchases**: Information on purchase habits centered around the perspective of a user. Meaning that for each registered user, the total expenditure both on products and shipping, the number of products bought and the variety as well as the total amount of orders placed is computed.
	* **user_web_analytics**: Analysis of the behaviour on the web by user. For each user it provides information on the amount of times it has visited the e-commerce site, speaking in percentages, how many of those visits ended up in a purchase, the mean number of products visited per session, percentage of products purchased after being visited and the mean expenditure per session.
	
  - Product
	* **product_sales**: Similar study to **user_purchases** but from the product perspective. Total sales per product, web visits to it, total units sold and number of times ordered.
	* **user_session**: Inforation regarding on the activity of the user on the web page. First time he/she visited it, last time and number of events of each type.

- **Incremental stages and snapshots**

As some of the tables in our data set would ingest new data on demand. Some of our models would need to be *incremental*,  it will be explained in detail in the Python scripts section but the silver stages that would be incremental were: **stg_addresses, stg_users, stg_events, stg_order_items and stg_orders**. 

Therefore the models in gold that use data from those stages would also need to be incremental, **except** when said models make *historical* aggregations (involving every record on the table). So, the incremental models in the gold layer are: **dim_shipping and fct_orders_products**.

Regarding the *snapshots*, while reading dbt documentation it was reccomended to use snapshots on SCDs. Taking this into account, among the tables that get new data, the ones in which it is more important to register changes are **users** and **addresses**, so snapshots for these models were created.
### Python scripts

As it was previously mentioned, the broad idea of the python scripts is being able to generate new data replicating a realistic route that a registered user would follow naturally. Then, update the bronze tables in Snowflake using the connector developped in Python for this purpose so this new data can be used during the dbt transformation.

Therefore the first step would be generating the data. Bear in mind that the objetive was to create it **automatically**, in order for the user to **not** have to create a new record by hand each time he/she wants to ingest new data.

- **create\_new\_records.py**

This is the script that creates the new data. It was decided that, in order to be able to maintain the integrity of the data throughout the tables in bronze while also creating the new data, we will **create new data in 5 tables in bronze: addresses, user, events, order_items and orders.**

The order was relevant as, to create the registers in some of the tables you need the key from another one, hence, the previously stated order. First we'll **create a new address** in *addresses*, **then a new user** with this address in *users*. For this purpose we use the random name and address generators in the libraries:

    import random_address  
    import randomname

and the MD5 hash generator from the internal python library **secrets**:

    import secrets
    hash = secrets.token_hex(nbytes=16)

Then we'll create the appropiate events in the *events* table. We want to simulate a natural route so we'll suppose that the user check out 3 products, adds one to the cart and purchases it. Therefore we'll create **3 page\_view events**, **1 add_to_cart event**, **1 checout event** and **1 package_shipped event**. All with the same user and session id. We'll use the product and order id to **create one new *order_items* record and one new order record in *records***.

- **upload_new_records.py**

This script would first uses the Snowflake connector to get all the tables in which we will insert new records and save them as csv.

    SNW_CHANGING_TABLES = ['addresses', 'users', 'events', 'order_items', 'orders']
    
    for snowflake_table in SNW_CHANGING_TABLES:  
	    query_out = snowflake_cursor.execute("select * from " + snowflake_table)  
	    file_name = DATA_TABLES_PATH + snowflake_table + '_snowflake.csv'
	    query_out.fetch_pandas_all().to_csv(file_name, index=False)

Then the new records for the tables are generated calling the script **create\_new\_records.py**. And save the resulting table in csv as well. Then both the snowflake table and the table with the new registers (as well as the old as it is similuting a transactional system) are compared to get just the new reocrds. This is done using **pandas**.

    # Find differences between snowflake table and internal
    table  
    df_snowflake = pd.read_csv(DATA_TABLES_PATH + old_data)  
    df_actual = pd.read_csv(DATA_TABLES_PATH + new_data)  
    new_file_name = DATA_TABLES_PATH + new_data.replace('.csv', '_new_records.csv')
      
    id_column = df_snowflake.columns[0] 
     
    df_new_records = df_actual[~df_actual.isin(df_snowflake)].dropna(how='all')  df_new_records = df_new_records.dropna(axis=0, subset=[id_column])

Finally, the new registers are uploaded to a **a table stage in Snowflake** and then **copied into the bronze table**. It is important to note that the table stage has the **PURGE = True** so the file in the stage is eliminated after uploading it, so we can use this code indefenite times.

    if not df_new_records.empty:  
        df_new_records.to_csv(new_file_name, index=False)  
        snowflake_cursor.execute("PUT file:/" + new_file_name.split(':', 2)[1].replace('\\', '//') + " @%" + new_data.replace('.csv', ''))  
        snowflake_cursor.execute("COPY INTO " + new_data.replace('.csv', '') + " FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) PURGE = TRUE")  
        os.remove(new_file_name)

## Future development

It is worth noting that while developing the automatic ingestion python scripts, several challenges were encountered. As a result, as stimulating and exciting that working on this project was, due to time constraints, there are a couple of development lines that were not completed.

- **Automatic continuous addition of new records**: The code as it is right is perfectly capable and ready tobe included in an automation routine **such as a dbt JOB** to be launched every X hours or daily so every day we can just check how the new records were added and used in our analysis in Snowflake.

- **Meteorological data**: A new table including data on the meteorological data on the registered address was being developed so it could be used to enrich the insights on the sales. More than a time constraint, there was an API configuration constraint. The API used to extract the data had a limited free usage that was burnout before retrieving all the necessary data to provide meaningful information to the project. Nonetheless the code is functional and present in **meteo_data.py** and can be checked out.
