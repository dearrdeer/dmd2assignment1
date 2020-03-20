import redis
import psycopg2
import datetime 
import decimal
import time

def run():
	start_time = time.time()
	redis_connection = redis.Redis(host='redis')

	categories = dict()

	ids = redis_connection.lrange("rental",0,-1)

	for id in ids:
		id = id.decode('utf-8')
		
		datestring = redis_connection.get("rental:{}:rental_date".format(id)).decode('utf-8')
		year = datetime.datetime.strptime(datestring, '%d-%b-%Y (%H:%M:%S.%f)').year
	
		if year != 2006:
			continue

		customer_id = redis_connection.get("rental:{}:customer_id".format(id)).decode('utf-8')
		inventory_id = redis_connection.get("rental:{}:inventory_id".format(id)).decode('utf-8')
		film_id = redis_connection.get("inventory:{}:film_id".format(inventory_id)).decode('utf-8')
		category_id = redis_connection.smembers("film_category:{}:film_id".format(film_id))

		categories.update({customer_id: categories.setdefault(customer_id, set()).union(category_id)})				

	customer_ids = categories.keys()

	t = (time.time() - start_time)

	for customer_id in customer_ids:
		if len(categories.get(customer_id)) > 1:
			first_name = redis_connection.get("customer:{}:first_name".format(customer_id)).decode('utf-8')
			last_name = redis_connection.get("customer:{}:last_name".format(customer_id)).decode('utf-8')
			print(first_name + ' ' + last_name)

	print("--- %s seconds ---" % t)