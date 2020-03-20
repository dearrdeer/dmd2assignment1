import redis
import psycopg2
import datetime 
import decimal
import time
import collections
import pandas
import sys

def retDefDict():
	return collections.defaultdict(int)

def run():
	

	co_starred = collections.defaultdict(retDefDict)
	
	start_time = time.time()
	redis_connection = redis.Redis(host='redis')

	actors = redis_connection.lrange("actor",0,-1)

	for actor in actors:
		
		actor_id = actor.decode('utf-8')
		films_of_actor = redis_connection.smembers("film_actor:{}:actor_id".format(actor_id))
		actor_name = redis_connection.get('actor:{}:first_name'.format(actor_id)).decode('utf-8') + redis_connection.get('actor:{}:last_name'.format(actor_id)).decode('utf-8')
		
		for film in films_of_actor:
			film_id = film.decode('utf-8')
			actors_of_film = redis_connection.smembers("film_actor:{}:film_id".format(film_id))

			for co_starred_actor in actors_of_film:
				
				co_starred_actor_id = co_starred_actor.decode('utf-8')
				co_starred_name = redis_connection.get('actor:{}:first_name'.format(co_starred_actor_id)).decode('utf-8') + redis_connection.get('actor:{}:last_name'.format(co_starred_actor_id)).decode('utf-8')

				if co_starred_actor_id == actor_id:
					continue

				co_starred[actor_name][co_starred_name] += 1

	t = time.time() - start_time
	
	
	df = pandas.DataFrame(co_starred)
	df = df.fillna(0)
	df = df.astype('int')
	print(df)
	print("--- %s seconds ---" % t)
