import redis
import psycopg2
import datetime 
import decimal
import time
import collections
import pandas
import os

def get_movies(actor_id, redis_connection):
	films = redis_connection.smembers("film_actor:{}:actor_id".format(actor_id))
	return list(films)

def get_actors(movie_id, redis_connection):
	actors = redis_connection.smembers("film_actor:{}:film_id".format(movie_id))
	return list(actors)

def run():

	start_time = time.time()
	redis_connection = redis.Redis(host='redis')

	start_actor_id = int(os.getenv("ID_ACTOR"))
	q = collections.deque()
	q.append(start_actor_id)
	deg_to_actor = collections.defaultdict(int)
	movie_flag = collections.defaultdict(bool)
	deg_to_actor[start_actor_id] = 0

	while len(q) > 0:
		
		cur_actor = q.popleft()
		films = get_movies(cur_actor,redis_connection)

		for f in films:
			f = int(f.decode("utf-8"))
			if movie_flag[f] == True:
				continue

			movie_flag[f] = True
			actors = get_actors(f,redis_connection)
			
			for a in actors:
				a = int(a.decode("utf-8"))
				if a != cur_actor and deg_to_actor[a] == 0:
					deg_to_actor[a] = deg_to_actor[cur_actor]+1
					q.append(a)

	t = time.time() - start_time
	
	actors = redis_connection.lrange("actor", 0 ,-1)
	for actor in actors:
		actor = int(actor.decode("utf-8"))
		name = redis_connection.get('actor:{}:first_name'.format(actor)).decode('utf-8') + redis_connection.get('actor:{}:last_name'.format(actor)).decode('utf-8')
		print(name + ' - ' + str(deg_to_actor[actor]))

	print("--- %s seconds ---" % t)