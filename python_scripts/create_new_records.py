import pandas as pd
import numpy as np
import random_address
import randomname
import secrets
import datetime
import csv
import random
from random import randint

GENERATOR_DATA_FOLDER = 'C:\\Users\\germa\\Desktop\\Proyecto_final\\python_scripts\\generator\\'

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def create_timestamp():
    timestamp = datetime.datetime.utcnow() + datetime.timedelta(hours=-8)
    formated_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    timestamp_format = datetime.datetime.strptime(formated_timestamp, '%Y-%m-%d %H:%M:%S.%f')

    return timestamp_format

def select_random_data(data_records, data_type):
    products = []
    for i in range(data_records):
        with open(GENERATOR_DATA_FOLDER + data_type + '_id.csv') as f:
            reader = csv.reader(f)
            chosen_row = random.choice(list(reader)[1:])
        products.append(chosen_row)

    return products

def select_data(data_id, data_type):
    all_data = pd.read_csv(GENERATOR_DATA_FOLDER + data_type + '_id.csv')
    return all_data.loc[all_data.iloc[:,0]==data_id]

def new_records_addresses():
    # Create a new single address record
    new_address = random_address.real_random_address_by_state('CA')

    ADDRESS_ID = secrets.token_hex(nbytes=16)
    ZIPCODE = new_address['postalCode']
    COUNTRY = 'United States'
    ADDRESS = new_address['address1']
    STATE = 'California'
    _FIVETRAN_DELETED = np.nan
    _FIVETRAN_SYNCED = create_timestamp()

    headers = ['ADDRESS_ID', 'ZIPCODE', 'COUNTRY', 'ADDRESS', 'STATE', '_FIVETRAN_DELETED', '_FIVETRAN_SYNCED']

    address_new_record = pd.DataFrame([[ADDRESS_ID, ZIPCODE, COUNTRY, ADDRESS, STATE, _FIVETRAN_DELETED, _FIVETRAN_SYNCED]], columns=headers)

    return address_new_record
def new_records_user(address_record):

    # Create timestamp from registration of user and generate first and last name
    creation_timestamp = create_timestamp()
    full_name = randomname.generate('names/surnames/english', 'names/people/computing', sep=' ')

    # Create new user record
    USER_ID = secrets.token_hex(nbytes=16)
    UPDATED_AT = creation_timestamp.replace(tzinfo=datetime.timezone.utc)
    ADDRESS_ID = address_record['ADDRESS_ID'].values[0]
    LAST_NAME = full_name.split()[0].upper()
    CREATED_AT = creation_timestamp.replace(tzinfo=datetime.timezone.utc)
    PHONE_NUMBER = str(random_with_N_digits(9))
    TOTAL_ORDERS = np.nan
    FIRST_NAME = full_name.split()[1].upper()
    EMAIL = LAST_NAME + '_' + FIRST_NAME + '@civicasoft.com'
    _FIVETRAN_DELETED = np.nan
    _FIVETRAN_SYNCED = creation_timestamp

    headers = ['USER_ID', 'UPDATED_AT', 'ADDRESS_ID', 'LAST_NAME', 'CREATED_AT', 'PHONE_NUMBER', 'TOTAL_ORDERS', 'FIRST_NAME', 'EMAIL', '_FIVETRAN_DELETED', '_FIVETRAN_SYNCED']

    user_new_record = pd.DataFrame([[USER_ID, UPDATED_AT, ADDRESS_ID, LAST_NAME, CREATED_AT, PHONE_NUMBER, TOTAL_ORDERS, FIRST_NAME, EMAIL, _FIVETRAN_DELETED, _FIVETRAN_SYNCED]], columns=headers)

    return user_new_record

def new_records_events(user_record):
    # Generate 5 event records -> 3 page_view, 1 checkout, 1 package shipped

    # Common parameters
    USER_ID = user_record['USER_ID'].values[0]
    SESSION_ID = secrets.token_hex(nbytes=16)
    ORDER_ID = secrets.token_hex(nbytes=16)
    _FIVETRAN_SYNCED = create_timestamp()

    headers = ['EVENT_ID', 'PAGE_URL', 'EVENT_TYPE', 'USER_ID', 'PRODUCT_ID', 'SESSION_ID', 'CREATED_AT', 'ORDER_ID', '_FIVETRAN_DELETED', '_FIVETRAN_SYNCED']

    # Page_view events
    # Get 3 products that are viewed
    products = select_random_data(3, 'products')
    page_view = []
    for product in products:
        EVENT_ID = secrets.token_hex(nbytes=16)
        PAGE_URL = r'https://greenary.com/product/' + product[0]
        EVENT_TYPE = 'page_view'
        PRODUCT_ID = product[0]
        CREATED_AT = create_timestamp()
        _FIVETRAN_DELETED = np.nan

        page_view.append([EVENT_ID, PAGE_URL, EVENT_TYPE, USER_ID, PRODUCT_ID, SESSION_ID, CREATED_AT, np.nan, _FIVETRAN_DELETED, _FIVETRAN_SYNCED])

    page_view_records = pd.DataFrame(page_view, columns=headers)

    # Add_to_cart event
    EVENT_ID = secrets.token_hex(nbytes=16)
    PAGE_URL = r'https://greenary.com/product/' + products[-1][0]
    EVENT_TYPE = 'add_to_cart'
    PRODUCT_ID = products[-1][0]
    CREATED_AT = create_timestamp()
    _FIVETRAN_DELETED = np.nan

    add_to_cart_record = pd.DataFrame([[EVENT_ID, PAGE_URL, EVENT_TYPE, USER_ID, PRODUCT_ID, SESSION_ID, CREATED_AT, np.nan, _FIVETRAN_DELETED, _FIVETRAN_SYNCED]], columns=headers)

    # Checkout event
    EVENT_ID = secrets.token_hex(nbytes=16)
    PAGE_URL = r'https://greenary.com/checkout/' + ORDER_ID
    EVENT_TYPE = 'checkout'
    CREATED_AT = create_timestamp()
    _FIVETRAN_DELETED = np.nan

    checkout_record = pd.DataFrame([[EVENT_ID, PAGE_URL, EVENT_TYPE, USER_ID, np.nan, SESSION_ID, CREATED_AT, ORDER_ID, _FIVETRAN_DELETED, _FIVETRAN_SYNCED]], columns=headers)

    # Package_shipped event
    EVENT_ID = secrets.token_hex(nbytes=16)
    PAGE_URL = r'https://greenary.com/shipping/' + ORDER_ID
    EVENT_TYPE = 'package_shipped'
    CREATED_AT = create_timestamp()
    _FIVETRAN_DELETED = np.nan

    package_shipped_record = pd.DataFrame([[EVENT_ID, PAGE_URL, EVENT_TYPE, USER_ID, np.nan, SESSION_ID, CREATED_AT, ORDER_ID, _FIVETRAN_DELETED, _FIVETRAN_SYNCED]], columns=headers)

    events_new_records = pd.concat([page_view_records, add_to_cart_record, checkout_record, package_shipped_record])

    return events_new_records

def new_records_order_items(events_records):
    # Create new order_items record
    ORDER_ID = events_records['ORDER_ID'].values[-1]
    PRODUCT_ID = events_records.loc[events_records['EVENT_TYPE']=='add_to_cart']['PRODUCT_ID'].values[0]
    QUANTITY =  random_with_N_digits(1)
    _FIVETRAN_DELETED = np.nan
    _FIVETRAN_SYNCED = create_timestamp()

    headers = ['ORDER_ID', 'PRODUCT_ID', 'QUANTITY', '_FIVETRAN_DELETED', '_FIVETRAN_SYNCED']

    order_items_new_record = pd.DataFrame([[ORDER_ID,PRODUCT_ID,QUANTITY,_FIVETRAN_DELETED,_FIVETRAN_SYNCED]], columns=headers)

    return order_items_new_record

def new_records_orders(events_records, user_record, order_items_record):
    # Get shipping service and promo information
    shipping_service = select_random_data(1, 'shipping_services')
    promo_id = select_random_data(1, 'promos')

    # Get product info
    product = select_data(events_records.loc[events_records['EVENT_TYPE']=='add_to_cart']['PRODUCT_ID'].values[0], 'products')

    # Create new order record
    ORDER_ID = events_records.loc[events_records['EVENT_TYPE']=='checkout']['ORDER_ID'].values[0]
    SHIPPING_SERVICE = shipping_service[0][1]
    SHIPPING_COST = randint(1*100, 10*100)/100
    ADDRESS_ID = user_record['ADDRESS_ID'].values[0]
    CREATED_AT = pd.Timestamp(events_records.loc[events_records['EVENT_TYPE']=='checkout']['CREATED_AT'].values[0]).replace(tzinfo=datetime.timezone.utc)
    PROMO_ID = promo_id[0][0]
    ESTIMATED_DELIVERY_AT = CREATED_AT + pd.to_timedelta(5, unit='D')
    ORDER_COST = order_items_record['QUANTITY'].values[0] * product['PRICE'].values[0]
    USER_ID = user_record['USER_ID'].values[0]
    ORDER_TOTAL = ORDER_COST + SHIPPING_COST
    DELIVERED_AT = np.nan
    TRACKING_ID = secrets.token_hex(nbytes=16)
    STATUS = 'shipped'
    _FIVETRAN_DELETED = np.nan
    _FIVETRAN_SYNCED = create_timestamp()

    headers = ['ORDER_ID', 'SHIPPING_SERVICE', 'SHIPPING_COST', 'ADDRESS_ID', 'CREATED_AT', 'PROMO_ID', 'ESTIMATED_DELIVERY_AT', 'ORDER_COST', 'USER_ID', 'ORDER_TOTAL', 'DELIVERED_AT', 'TRACKING_ID', 'STATUS', '_FIVETRAN_DELETED', '_FIVETRAN_SYNCED']

    order_new_record = pd.DataFrame([[ORDER_ID,SHIPPING_SERVICE,SHIPPING_COST,ADDRESS_ID,CREATED_AT,PROMO_ID,ESTIMATED_DELIVERY_AT,ORDER_COST,USER_ID,ORDER_TOTAL,DELIVERED_AT,TRACKING_ID,STATUS,_FIVETRAN_DELETED,_FIVETRAN_SYNCED]], columns=headers)

    return order_new_record

if __name__ == "__main__":
    new_address = new_records_addresses()
    new_user = new_records_user(new_address)
    new_events = new_records_events(new_user)
    new_order_items = new_records_order_items(new_events)
    new_order = new_records_orders(new_events, new_user, new_order_items)
    print(new_user)

