#  CMPT 474 Spring 2014, Assignment 5 boilerplate

# Imports from standard library
import os
import sys
import time
import json
import StringIO

# Imports from installed libraries
import redis
import mimeparse
from bottle import route, run, request, response, abort

# Imports from boilerplate
from vectorclock import VectorClock

DEBUG = False

config = { 'servers': [{ 'host': 'localhost', 'port': 6379 }] }

if (len(sys.argv) > 1):
	config = json.loads(sys.argv[1])

output = sys.stdout

# Connect to a single Redis instance
client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)

# A user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "clock": { "c1" : 5, "c2" : 3 } }' http://localhost:2500/rating/bob
# Response is a JSON object specifying the new rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):

	# Check to make sure JSON is ok
	type = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
	if not type: return abort(406)

	# Check to make sure the data we're getting is JSON
	if request.headers.get('Content-Type') != 'application/json': return abort(415)

	response.headers.append('Content-Type', type)
	
	# Read the data sent from the client
	data = json.load(request.body)
	recieved_rating = data.get('rating')
	recieved_vc = VectorClock.fromDict(data.get('clocks'))

	# Basic sanity checks on the rating
	if isinstance(recieved_rating, int):recieved_rating = float(recieved_rating)
	if not isinstance(recieved_rating, float): return abort(400)

	# Weave the new rating into the current rating list
	key = '/rating/'+entity
	
	tea_name = entity
	
	# COMPUTE THE MEAN, finalrating after converge existing and recieving value
	finalrating, choices, new_vc = vector_converge(tea_name,recieved_rating,recieved_vc)

	# SET THE RATING, CHOICES, AND CLOCKS IN THE DATABASE FOR THIS KEY
	if choices!=None:
		put_to_redis(tea_name, finalrating,choices,new_vc) #store new score
	
	# Return the new rating for the entity
	return {
		"rating": finalrating
	}
#converge using vector clock
def vector_converge(tea_name,r_rating, r_vc):
	if DEBUG:
		print "[new recieved]tea_name:" , tea_name, "rating:", r_rating,  "new v clocks:",r_vc

	rating, choices, vc = get_from_redis(tea_name)
	if rating == None: # no key found for the tea
		conv_rating = r_rating
                conv_choices = [r_rating]
                conv_vc =  r_vc
        else:
		if DEBUG:
			print "[previous]tea_name:" , tea_name, "rating:", rating,"choices:", choices, "new v clocks:",vc
		#compare existing clocks and received clock	
		#print "r_vc = " , r_vc.clock , "vc=", vc.clock ,
		if DEBUG:
			print "[compare] r_vc == vc :", (r_vc == vc)
			print "[compare] r_vc > vc: " , (r_vc > vc)
			print "[compare] r_vc < vc: " , (r_vc < vc)
			print "[compare] r_vc >= vc: " ,( r_vc >= vc)
			print "[compare] coalesce = " , VectorClock.coalesce([r_vc,vc])
			print "[compare] converge = " , VectorClock.converge([r_vc,vc])
		if r_vc == vc:
			conv_rating = r_rating
                	conv_choices = None
                	conv_vc =  None
		elif r_vc > vc: # more recent data
			#compute mean value
			conv_rating = r_rating 
			#choose the recent vector clocks
			conv_vc = r_vc
			#choose the recent choices based on the clocks 
			conv_choices = [r_rating] 
		elif r_vc < vc: # ignore 
			if DEBUG:
				print "[ignore] r_vc<vc"
			#compute mean value
                        conv_rating = rating
                        #choose the recent vector clocks
                        conv_vc = None
                        #choose the recent choices based on the clocks
                        conv_choices = None
	
		else:
			combined_clocks_list = VectorClock.coalesce([r_vc,vc])
			if DEBUG:
				print "combined clocks:",combined_clocks_list
			
			if r_vc in combined_clocks_list and vc in combined_clocks_list:	
				combined_clocks_list.sort()			
				#choose merged choices based on the clocks
				conv_choices = choices + [r_rating]
				
				conv_rating = meanAvg(conv_choices)
                        	#choose the recent vector clocks
                        	conv_vc = VectorClock.converge([r_vc,vc]) 
				if DEBUG:
					print "[incom] c_choices=", conv_choices, "c_rating=",conv_rating, "c_vc=", conv_vc						
			
	
	return conv_rating, conv_choices,conv_vc
        
#put format
#rating = float ex 2.3
#choices =  dictionay ex) [1,2]
#clocks = vc.clock  ex) {'c1': 10, 'c0': 7} 
def put_to_redis(tea_name, rating, dic_choices, vc):
	json_data = result({ 'rating':rating,'choices':dic_choices, 'clocks': vc.clock })
	if DEBUG:
                print "[put to redis]tea_name:" , tea_name, "rating:", rating, "choices:", dic_choices, "v clocks:", vc
		print "[put to redis]json_data:", json_data

	client.set("tea:%s:json" % tea_name, json_data) #insert json data
 
# return format
# -- rating(float)     ex) 5.0
# -- choices(dictionar) ex) [1,3]
# -- vc(VectorClock)
# if there is no key for the tea,
# -- reurn None,None,None
# Original JSON Format:  {"rating": 1.0, "clocks": {"c0": 1}, "choices": [10, 20]}
def get_from_redis(tea_name):

        try:
                data = eval(client.get("tea:%s:json" % tea_name))
        except:
                #print "*** Error [get from redis] no key found for the tea:" + tea_name
                return None,None,None

        try:
                rating = data["rating"]
        except:
                rating = data["rating"]

        choices = data["choices"]
        clocks = data["clocks"]  #vc.clock, ex) {'c1': 10, 'c0': 7} 
        	
	return rating, choices, VectorClock.fromDict(clocks)
        
def meanAvg(choices):
        sumRating =0
        counter=0
        for i in choices:
                sumRating +=i
                counter +=1
        #print sumRating/counter
        return sumRating/counter

#json format
# -- choices: [10,20]
# -- clocks: [ { "c0": 5 }, { "c1": 3 } ]
def result(r):
       #print "jsondumps:" , json.dumps(r)
       return json.dumps(r)
        

# Add a route for getting the aggregate rating of something which can be accesed as:
# curl -XGET http://localhost:2500/rating/bob
# Response is a JSON object specifying the rating list and time list for the entity:
# { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
@route('/rating/<entity>', method='GET')
def get_rating(entity):
     	rating, choices, vc = get_from_redis(entity)
	#if DEBUG:
        print "[get_rating]entity:", entity, "rating:",format(rating, '.2f').rstrip('0').rstrip('.')

	return { "rating":format(rating, '.2f').rstrip('0').rstrip('.') , 
		 "choices": result(choices), 
		 "clocks":result([vc.clock])
	}
	
# Add a route for deleting all the rating information which can be accessed as:
# curl -XDELETE http://localhost:2500/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
# { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
	print "[del rating]entity:", entity
	#count = client.delete('/rating/'+entity)
	count = client.delete("tea:%s:json" % entity)
	if count == 0: return abort(404)
	return { "rating": None }

# Fire the engines
if __name__ == '__main__':
	run(host='0.0.0.0', port=os.getenv('PORT', 2500), quiet=True)
