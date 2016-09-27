import bs4, json
import requests
import json
import sys, os
import pika, re
from pymongo import MongoClient
from redbrain import Parser as YtParser
from categorizer import categorize

requests.packages.urllib3.disable_warnings()

# visited = {}
# linkQ  = Queue()
# linkQ.put("UCO5rwjHY-jcX-gmzOCSApOQ", 100)
# linkQ.put("HEARTROCKERChannel", 100)
# linkQ.put("FoodTravelTVChannel", 100)
# linkQ.put("UCQ0-okjX18v85QlCAr1GBwQ", 100)
# linkQ.put("easycooking", 100)
# linkQ.put("VrzoChannel", 100)
# linkQ.put("GGTKcastation", 100)
# linkQ.put("bomberball", 100)
# linkQ.put("UC7rtE7hSTaC8xDf5v_7O1qQ", 100)
# linkQ.put("UC0TnoMtL2J9OsXU4jgvO_ag", 100)
# linkQ.put("UClshsyv7mLwBxLLfSSwLAFQ", 100)
# linkQ.put("llookatgreeeen", 100)
# linkQ.put("faharisara", 100)
# linkQ.put("ninabeautyworld", 100)

# # result = []
# while True:
#     q = linkQ.get()
#     try:
#         x = parseChannelByIdOrUser(q, linkQ, visited)
#         print("Remaining in Q", linkQ.qsize())
#         MLAB_API_KEY = "ucQuRaICqjzsxmtTVyuXp3dxzNheiKmy";
#         MLAB_TEMP_COLLECTION = "profilesv5"
#         mongoUri = "https://api.mlab.com/api/1/databases/alphastoka/collections/" + MLAB_TEMP_COLLECTION + "/?apiKey=" + MLAB_API_KEY
#         r=  requests.post(mongoUri, headers={
#             "Content-Type" : "application/json"
#             }, data=json.dumps(x))

#         print(r.status_code)
#     except Exception as ex:
#         print(ex)

# this is for the actual crawling
class StokaInstance:

    def __init__(self, rabbit_mq_connection, yt_user, group_name="default_stoka"):
        self.group_name = group_name;
        self.queue_name = group_name + "_yt";
        self.rabbit_channel = rabbit_mq_connection.channel();
        self.rabbit_channel.queue_declare(queue=self.queue_name,durable=True)
        self.mongo_client = MongoClient("mongodb://54.169.89.105:27017")
        self.mongo_db = self.mongo_client['stoka_' + self.group_name]
        self.mongo_system = self.mongo_client['stoka_system']

        for doc in self.mongo_system.categorizer.find({}).skip(0).limit(1):
            del doc["_id"]
            self.categorizer_kwd = doc
            break

        #seed the queue
        # seed_user_obj = self.get_user(yt_user)
        self.seed_user = yt_user
        # print(seed_user_obj)
        self.astoka_progress = 0
        self.astoka_error = 0
        self.pushQ(yt_user)

        
    STORAGE = {}
    Q = []

    #
    # Procesisng of the object in each iteration of pop()
    # object = User object (contains id, and username etc.)
    #
    def process(self, id):
        try:
            self.save(self.get_user(id))
            self.astoka_progress = self.astoka_progress + 1
        except Exception as ex:
            print(ex)
            self.astoka_error = self.astoka_error + 1
            print("[o] skipped", id)

        print("@astoka.progress ", self.astoka_progress)
        print("@astoka.error ", self.astoka_error)

    # persist to mongodb
    def save(self, object):
        print(object)
        # short term memory checking if we have seen this
        self.STORAGE[object["id"]] = True
        object["_seed_username"] = self.seed_user
        object["_dna"] = "stoka-yt"
        object["predicted_age"] = 0
        object["category"] = {}

        confidence = categorize(str(object["description"]), self.categorizer_kwd)
        object["category"] = confidence

        try:
            result = self.mongo_db.youtube.insert_one(object)
            print("[x] Persisting %s (%s) / mongoId -> %s" % (object["id"], object["title"], result.inserted_id))
        except Exception as ex:
            print(ex)
            self.astoka_error = self.astoka_error + 1
            print("[o] Exception while saving to mongo (might be duplicate)")

    
    # check if it's in mongo or in some sort of fast memoryview
    # this is for preventing dupe , it's not 100% proof but it's better than nthing
    def inStorage(self, idOrUser):
        return idOrUser in self.STORAGE

    # push object to work queue
    # so other can pick up this object and populate the queue
    # with the object's follower
    def pushQ(self, idOrUser):
        self.rabbit_channel.basic_publish(exchange='',
                      routing_key=self.queue_name,
                      body=idOrUser,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, 
                      ))
        print("[x] Sent to ", self.queue_name, idOrUser)
        #self.Q.append(object)
    
    ## Called on pop done
    # this is async pop callback
    def _rabbit_consume_callback(self,ch, method, properties, body):
        # print(" [x] Received %r" % (body,))
        ch.basic_ack(delivery_tag = method.delivery_tag)
        # p = json.loads(body.decode("utf-8") )

        idOrUser = body.decode("utf-8")
        parser = YtParser()
        (channel, neighbors) = parser.parseChannelByIdOrUser(idOrUser, True)

        print("[x] Working on ", channel["title"], idOrUser)
        
        for f in neighbors:
            if self.inStorage(f):
                continue
            
            self.process(f)
            print("- ", f)
            self.pushQ(f)
            

    # popping (called once)
    def popQ(self):
        self.rabbit_channel.basic_qos(prefetch_count=1)
        self.rabbit_channel.basic_consume(self._rabbit_consume_callback,
                      queue=self.queue_name)
        # this is blocking (forever)
        self.rabbit_channel.start_consuming()
    

    def get_user(self, id_or_username):
        print("[x] Getting ID", id_or_username)
        parser = YtParser()
        (channel, neighbors) = parser.parseChannelByIdOrUser(id_or_username, False)

        return channel

    # entry point
    def run(self):
        #do work!
        self.popQ()


if __name__ == '__main__':
    RABBIT_USR = os.getenv('RABBIT_USR', "rabbitmq")
    RABBIT_PWD = os.getenv('RABBIT_PWD', "Nc77WrHuAR58yUPl")
    RABBIT_PORT = os.getenv('RABBIT_PORT', 32774)
    RABBIT_HOST = os.getenv('RABBIT_HOST', 'localhost')
    SEED_ID = os.getenv('SEED_ID', 'NinaBeautyWorld')
    GROUP_NAME = os.getenv('GROUP_NAME', 'testyt6')

    print("1138K-RMA2")
    print("using configuration", RABBIT_HOST, RABBIT_PWD, RABBIT_USR, int(RABBIT_PORT))

    credentials = pika.PlainCredentials(RABBIT_USR, RABBIT_PWD)
    print("Connecting to Rabbit..")
    connection = pika.BlockingConnection(pika.ConnectionParameters(
               RABBIT_HOST, port=int(RABBIT_PORT), credentials=credentials))
            
    print("Starting Stoka (YouTube)..")
        
    instance = StokaInstance(connection,yt_user=SEED_ID, group_name=GROUP_NAME)
    
    instance.run()
