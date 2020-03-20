import redis
import psycopg2
import datetime 
import decimal
import time
import collections
import pandas

def run():

	start_time = time.time()
	redis_connection = redis.Redis(host='redis')

	rents = redis_connection.lrange('rental', 0, -1)
	report = collections.defaultdict(int)
	s = 0

	for rent in rents:
		rent_id = rent.decode('utf-8')
		inventory_id = redis_connection.get('rental:{}:inventory_id'.format(rent_id)).decode('utf-8')
		film_id = redis_connection.get('inventory:{}:film_id'.format(inventory_id)).decode('utf-8')
		report[film_id] += 1
	

	films = redis_connection.lrange('film', 0, -1)

	t = time.time() - start_time
	
	for film in films:
		film_id = film.decode('utf-8')
		film_name = redis_connection.get('film:{}:title'.format(film_id)).decode('utf-8') 
		category_id = redis_connection.smembers('film_category:{}:film_id'.format(film_id)).pop().decode('utf-8')
		category = redis_connection.get('category:{}:name'.format(category_id)).decode('utf-8')
		rented = report[film_id]
		print('Movie: ' + film_name + ' Category: ' + category + ' Rented: ' + str(rented))

	print("--- %s seconds ---" % t)