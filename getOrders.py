import requests
from lib import create_table, push_data_to_redshift, get_creds


def get_order_data_from_shopify():
    """
    Get orders from shopify and returns
    :return:Orders dictionary object
    """
    try:
        shopify_cred = get_creds('shopify')
        url, api_key, pwd = shopify_cred['store_url'], shopify_cred['api_key'], shopify_cred['password']
        r = requests.get(url=url + 'admin/orders.json', auth=(api_key, pwd))
        if r.status_code == 200:
            return r.json()['orders']
        else:
            return None
    except Exception as e:
        print("Exception {} occured".format(e))
        return None


def data_processor(ord_data):
    """
    Processes input dataset and generates data models with date,count,net and gross
    :return: Parsed order data model dictionary
    """
    orders_to_redshift = {}
    for ord in ord_data:
        created_date = ord['created_at'].split('T')[0]
        if created_date in orders_to_redshift.keys():
            orders_to_redshift['orders counts'] += 1
            orders_to_redshift['gross'] += float(ord['total_price'])
            orders_to_redshift['net'] += float(ord['subtotal_price'])
        else:
            orders_to_redshift[created_date] = {
                'orders counts': 1,
                'gross': float(ord['total_price']),
                'net': float(ord['subtotal_price'])
            }
    return orders_to_redshift


table_name = "order_data"
order_data_from_shopify = get_order_data_from_shopify()
create_table(table_name=table_name)
push_data_to_redshift(order_data_from_shopify, table_name)
order_summary = data_processor(order_data_from_shopify)
for dt, ds in order_summary.items():
    print("DATE : {}, Total order count : {}, Total Gross amount : {}, Total net amount : {}".format(dt, ds[
        'orders counts'], ds['gross'], ds['net']))
