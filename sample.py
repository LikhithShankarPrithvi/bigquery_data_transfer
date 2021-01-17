
from google.cloud import bigquery
import json
# from bson.objectid import ObjectId
import pymongo
import bson
from celery import Celery
# Construct a BigQuery client object.
client = bigquery.Client()

broker_url='amqp://localhost//'
backend_url='mongodb+srv://student0:<password>@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority'
app = Celery('app', broker=broker_url, backend='mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority')

client_mongo = pymongo.MongoClient("mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority")
# @app.task
# def extract(client):

#     total video watched all devices per user
#         {u1 -> <total_video_hours>}
#         u1-> 10, u2 -> 10
#     total video watched per device, per user
#         {u1 -> {d1-> 3, d2 -> 4, d4->7}, u2-> {d3 -> 10}}
#     device video percentage
#         {u1 -> {d1 -> 30, d2 -> 40, d4->70}, u2 -> {d3->100}}
#     maximum device video percentage
#         {u1-> 70, u2 -> 100}

#UserSharing collection{“user_id”: “u1”,“year”: 2021,“week”: 1,“total_video_hours_watched” : 100,“total_video_per_devices”:[        {"device" : "d1"        "count" : 70},        {"device" : "d1"        “count" : 30},    ],“device_video_percent”:    [        {"device" : "d1"        "percentage" : 70},        {"device" : "d1"        "percemtage" : 30},    ],“max_device_video_percent” : 70,

global user_device_map
user_device_map={}
user_map={}
user_device_percentage_map={}
user_maximum_device_percentage={}


db=client_mongo.school_db
user_sharing=db.user_sharing

query = """
SELECT * 
FROM `sky786.demo.video_last_week` 
LIMIT 1000
"""
query_job = client.query(query) # Make an API request.
for row in query_job:
    #print(row['user_id'],row['device_id'],row['duration'],row['year'],row['week'])
    user_id=row['user_id']
    device_id=row['device_id']
    duration=float(row['duration'])
    year=row['year']
    week=row['week']
    # calculating user map
    if user_id in user_map:
        user_map[user_id]+=duration
    else:
        user_map[user_id]={}
        user_map[user_id]=duration


    # calculating user device map
    if user_id in user_device_map:
        user_device_map[user_id][device_id]=duration
    else:
        user_device_map[user_id]={}
        user_device_map[user_id][device_id]=duration

for user_id,device_map in user_device_map.items():
    #print(user_id,device_map)
    maximum_device_percentage=0
    for device_id,duration in device_map:
        #print(device_id,duration)
        percentage=(float(duration)/user_map[user_id])*100
        if user_id in user_device_percentage_map:
            user_device_percentage_map[user_id][device_id]=percentage
        else:
            user_device_percentage_map[user_id]={}
            user_device_percentage_map[user_id][device_id]=percentage
        maximum_device_percentage=max(maximum_device_percentage,percentage)
        user_maximum_device_percentage[user_id]=maximum_device_percentage

#        {"device" : "d1"        "count" : 70},        {"device" : "d1"        “count" : 30},    ],
        
for user_id in user_map.keys():
    total_video_per_devices=[]
    for device_id,duration in user_device_map[user_id]:
        video_per_device={
            "device":device_id,
            "count":duration
        }
        total_video_per_devices.append(video_per_device)

    user_sharing_doc={
        "user_id":user_id,
        "year": 2021,
        "week": 1,
        "total_video_hours_watched" : user_map[user_id],
        "total_video_per_devices": total_video_per_devices,
        "maximum_device_percentage":user_maximum_device_percentage[user_id]
    }
    print(user_sharing_doc)
    user_sharing.insert_one(user_sharing_doc)


print("User device map",user_device_map)
print("User map:",user_map)
print("User Device percentage map:",user_device_percentage_map)
print("User maximum device percentage",user_maximum_device_percentage)



# INSERT INTO `sky786.demo.video_last_week`
# VALUES ('u1','d1',3,2021,1),('u1','d2',4,2021,1),('u1','d4',3,2021,1),('u2','d3',5,2021,1),('u2','d5',5,2021,1)