import psycopg2
import json

table_create = """
create table if not exists {}(id  varchar,
created_at  varchar,
total_price  varchar,
subtotal_price  varchar,
total_tax  varchar,
total_line_items_price  varchar,
total_price_usd  varchar,
financial_status  varchar,
currency  varchar,
app_id  varchar,
total_tip_received  varchar,
user_id  varchar,
order_number  varchar,
total_discounts  varchar,
updated_at  varchar,
processing_method  varchar,
processed_at  varchar,
token  varchar,
name  varchar);
"""

required_data = ['id', 'created_at', 'total_price', 'subtotal_price', 'total_tax', 'total_line_items_price',
                 'total_price_usd', 'financial_status', 'currency', 'app_id',
                 'total_tip_received', 'user_id', 'order_number', 'total_discounts', 'updated_at', 'processing_method',
                 'processed_at', 'token', 'name']




def get_creds(type):
    with open('user.config') as f:
        data = json.load(f)
    return data[type]


def create_table(table_name):
    red_shift_cred = get_creds('redshift')
    db_name, host, usr, pwd, port = red_shift_cred['db_name'], red_shift_cred['host'], red_shift_cred['user'], \
                                    red_shift_cred['password'], red_shift_cred['port']
    con = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, host, port, usr, pwd))
    cur = con.cursor()
    cur.execute(table_create.format(table_name))
    con.commit()
    con.close()


def get_insert_query(table_name, order, keys=required_data):
    columns = ', '.join(keys)
    values = "'" + "', '".join([str(order[k]) for k in keys]) + "'"
    qry = 'insert into %s (%s) Values (%s);' % (table_name, columns, values)
    print("INSERT QUERY : {}".format(qry))
    return qry


def push_data_to_redshift(order_data, table_name):
    """
    In progress
    :param odr_parse_result:
    :return:
    """
    red_shift_cred = get_creds('redshift')
    db_name, host, usr, pwd, port = red_shift_cred['db_name'], red_shift_cred['host'], red_shift_cred['user'], \
                                    red_shift_cred['password'], red_shift_cred['port']
    con = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, host, port, usr, pwd))
    cur = con.cursor()
    for ord in order_data:
        qry = get_insert_query(table_name, ord, required_data)
        cur.execute(qry)
    con.commit()
    con.close()

