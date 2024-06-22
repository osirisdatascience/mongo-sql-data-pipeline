import pymssql
from pymongo import MongoClient
from datetime import datetime, timedelta


#  abstracting a function to return data from the Data Lake MongoDB
def query_mongo_datalake(aggregation, conn):

    client = MongoClient(conn['MONGO_IP']) 
    db = client[conn['DB_NAME']]
    collection = db[conn['COLLECTION']]

    results = collection.aggregate(aggregation)

    return results

#  function to truncate the table 
def truncate_table_sql_server_DW(table_name, db, connx):

    try:
        conn = pymssql.connect(
            server=connx['DBIP'],
            user=connx['DBLOGIN'],
            password=connx['DBPASSWORD'],
            database=db,
            autocommit=True
        )   
        cursor = conn.cursor()  
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        cursor.execute(truncate_sql)   
        conn.commit()
        
    except Exception as e:
        print(e)
        conn.rollback() # In case of error, apply the rollback clause
        
    finally:
        # Close the connection in the end  
        conn.close()

#  abstracting a function to insert data into the DW SQL server
def insert_data_sql_server_DW(sql, db, connx):

    try:

        # establish the connection
        conn = pymssql.connect(
            server=connx['DBIP'],
            user=connx['DBLOGIN'],
            password=connx['DBPASSWORD'],
            database=db,
            autocommit=True
        )   
        # build a cursor
        cursor = conn.cursor()  
        # execute a SQL instruction
        cursor.execute(sql)   
        # confirm the transaction
        conn.commit()
        
    except Exception as e:
        print(e)
        conn.rollback() # in case of error, apply the rollback clause
        
    finally:

        # close the connection in the end  
        conn.close()


def main():

    # datalake mongo  
    DL_MONGO_CONN = { 'MONGO_IP': '0.0.0.0', 
                    'DB_NAME':'general_sales_from_system',
                    'COLLECTION':'collectionname'}  
    
    # dw sql credentials
    DW_SQL_CONN = {'DBIP':'0.0.0.0',
            'DBLOGIN' : 'login',
            'DBPASSWORD' : 'password'}


    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)

    #  mongo query to return all the sales per country with value, quantity and more details
    AGGREGATION = [
        {"$match": {
            "order_date": {"$gte": thirty_days_ago},
            "status": "delivered",
            "order_value": {"$gt": 300}
        }},
        {"$group": {
            "_id": "$customer.address.country",
            "amount_paid": {"$sum": "$order_value"},
            "total_orders": {"$sum": 1}
        }},
        {"$sort": {"amount_paid": -1}},
        {"$project": {
            "_id": 0,
            "country": "$_id",
            "amount_paid": 1,
            "total_orders": 1
        }}
    ]

    result_from_datalake = query_mongo_datalake(aggregation= AGGREGATION, conn = DL_MONGO_CONN)

    # cleaning the previous data and updating the information with the new sales
    truncate_table_sql_server_DW(table_name='sales_analysis_county', db='sales_db', connx = DW_SQL_CONN)


    #  loop to insert the sales data per country, in the DW SQL, to use in a possible Data Viz Project
    for i in result_from_datalake:

        country = i['country']
        amount_paid = i['amount_paid']
        total_orders = i['total_orders']

        mean_value_per_order = i['amount_paid'] // i['total_orders']

        sql_insert_dw = f""" INSERT INTO sales_db..sales_analysis_county 
                                country, amount_paid, total_orders, mean_per_order
                                VALUES(
                                    {country},
                                    {amount_paid},
                                    {total_orders},
                                    {mean_value_per_order}
                                    ) """

        insert_data_sql_server_DW(sql = sql_insert_dw, db = 'sales_db' ,connx = DW_SQL_CONN)


        print(f"Data inserted about {i.country} sales amount")
