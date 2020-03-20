import redis
import psycopg2
import datetime 
import decimal
import time
import query1, query2, query3, query4, query5 

def migrate_table(redis_connection, postgre_connection, table_name):
    cur = postgre_connection.cursor()
    cur.execute("SELECT * FROM {}".format(table_name))
    rows = cur.fetchall()
    
    colnames = [desc[0] for desc in cur.description]
    for row in rows:
  
        id = row[0]
        redis_connection.rpush("{}".format(table_name), id)

        for i in range(1,len(colnames)):
            field = row[i]
            if field is None or type(field) is bool:
                continue
            
            if type(field) == datetime.datetime or type(field) == datetime.date:
                field = field.strftime("%d-%b-%Y (%H:%M:%S.%f)")
            
            if type(field) == decimal.Decimal:
                field = float(field)
            
            if type(field) is list:
                for j in field:
                    redis_connection.rpush("{}:{}:{}".format(table_name, id,colnames[i]), j)
                continue

            if type(field) is memoryview:
                field = field.tobytes()

            redis_connection.set("{}:{}:{}".format(table_name, id,colnames[i]), field)



def make_relations(redis_connection, postgre_connection, table_name):
    cur = postgre_connection.cursor()
    cur.execute("SELECT * FROM {}".format(table_name))
    rows = cur.fetchall()

    colnames = [desc[0] for desc in cur.description]

    for row in rows:
        first_id = row[0]
        second_id = row[1]    #film_actor #film_category

        redis_connection.sadd("{}:{}:{}".format(table_name,first_id, colnames[0]), second_id)
        redis_connection.sadd("{}:{}:{}".format(table_name,second_id, colnames[1]), first_id)




if __name__ == '__main__':
    redis_connection = redis.Redis(host='redis')
    
    s = "dbname='%s' user='%s' host='%s' password='%s'" % ('dvdrental', 'postgres', 'postgres','postgres')

    while(True):
        try:
            postgre_connection = psycopg2.connect(s)
            break
        except Exception as e:
            time.sleep(5)
    
    
    tables = ['actor', 'address', 'category', 'city', 'country', 'customer', 'film', 'inventory', 'language', 'payment', 'rental', 'staff', 'store']

    for table in tables:
        print("Moving {} table".format(table), flush=True)
        migrate_table(redis_connection, postgre_connection, table)
    
    print("Making relations")
    make_relations(redis_connection, postgre_connection, "film_actor")
    make_relations(redis_connection, postgre_connection, "film_category")
    print("Done!")
    
    Queries = [query1, query2, query3, query4, query5]

    for i in range(0,5):
        print()
        print("                QUERY{}                ".format(i+1))
        print()
        Queries[i].run()

        

