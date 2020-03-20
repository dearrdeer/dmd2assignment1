import redis
import psycopg2
import datetime 
import decimal
import time
import collections
import pandas
import os

def run():

	start_time = time.time()
	redis_connection = redis.Redis(host='redis')

	start_costumer_id = int(os.getenv("ID_CUSTOMER"))

	rents = redis_connection.lrange('rental', 0, -1)

	films_of_costumer = collections.defaultdict(set)
	
	for rent in rents:
		rent_id = rent.decode('utf-8')
		inventory_id = redis_connection.get('rental:{}:inventory_id'.format(rent_id)).decode('utf-8')
		film_id = int(redis_connection.get('inventory:{}:film_id'.format(inventory_id)).decode('utf-8'))
		costumer_id = int(redis_connection.get('rental:{}:customer_id'.format(rent_id)).decode('utf-8'))
		films_of_costumer[costumer_id].add(film_id)


	costumers = redis_connection.lrange('customer', 0, -1)
	start_films = films_of_costumer[start_costumer_id]
	score_of_film = collections.defaultdict(int)

	for costumer in costumers:
		costumer_id = int(costumer.decode('utf-8'))
		
		if costumer_id == start_costumer_id:
			continue

		current_films = films_of_costumer[costumer_id]
		common_films = current_films.intersection(start_films)
		score = len(common_films)
		difference = current_films.difference(start_films)
		
		for film in difference:
			score_of_film[film] += score

	ans = score_of_film.items()
	answer = sorted(ans, key = lambda ans: ans[1])
	answer.reverse()
	
	t = time.time() - start_time
	

	for i in range(50):
		movie = redis_connection.get('film:{}:title'.format(answer[i][0])).decode('utf-8')
		print("Movie: {} - Score: {}".format(movie, answer[i][1]))

	print("--- %s seconds ---" % t)