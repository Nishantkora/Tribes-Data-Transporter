__author__ = 'Nishanth'
from google.cloud import storage
from gremlin_python.driver import client, serializer
import os
from os import environ
import json

environ['GOOGLE_APPLICATION_CREDENTIALS'] = environ['CREDENTIAL']
client = client.Client(environ['ENDPOINT'], 'g', username=environ['USERNAME'], password=environ['PASSWORD'],message_serializer=serializer.GraphSONSerializersV2d0())

class TribesDataTransporter:
    #Initialize the data
    def __init__(self, transform_type):
        self.transform_type = transform_type
        self.bucket_name = environ['BUCKETNAME'] #"data-engineering-intern-data"
        self.vertex = "g.addV('Entity').property('id', '{}').property('Name', '{}')"
        self.relationship = "g.V('{}').addE('{}').to(g.V('{}'))"
        self.last_upd_sync = "2021-01-14 11:32:26" # needs to be stored in db somehwere to maintain till last sync of files has been completed
        self.get_bucket_objects()

    # Get the bucket Objects
    def get_bucket_objects(self):
        print("Getting the bucket Details")
        storage_client = storage.Client()
        print(self.bucket_name)
        blobs = storage_client.list_blobs(self.bucket_name)
        if self.transform_type == "full_sync":
            self.full_sync(blobs)
        if self.transform_type == "partial_sync":
            self.partial_sync(blobs)

    # store file data in azure cosmos db
    def load_data_to_graphdb(self, blob_class):
        print("Loading the JSON data")
        file_string = blob_class.download_as_text()
        graph_data_list = json.loads(file_string)
        for node_rel in graph_data_list:
            if node_rel['Kind'] == "node":
                print("Processing the Node")
                name = node_rel['Property']['Name'] if 'Name' in node_rel['Property'] else  node_rel['Property']['NameLower']
                vertex = self.vertex.format(node_rel['Property']['IdMaster'], name)
                try:
                    callback = client.submitAsync(vertex)
                    if callback.result() is not None:
                        results = callback.result()
                    if results is not None:
                        print("\tSuccess!")
                    else:
                        print("Something went wrong with this query: {0}".format(vertex))
                except Exception as e:
                    print('There was an exception: {0}'.format(e))
            if node_rel['Kind'] == "relationship" and node_rel["DeDuplication"] in ["FALSE", "TRUE"]:
                print("Processing the relationship")
                edge = self.relationship.format(node_rel['FromIdObject'], node_rel['Type'], node_rel['ToIdObject'])
                try:
                    callback = client.submitAsync(edge)
                    if callback.result() is not None:
                        results = callback.result()
                    if results is not None:
                        print("\tSuccess!")
                    else:
                        print("Something went wrong with this query: {0}".format(edge))
                except Exception as e:
                    print('There was an exception: {0}'.format(e))


    # sync all files at initial levels
    def full_sync(self, blobs):
       for blob in blobs:
           print("iterating the blobs")
           if blob.content_type == 'application/json':
               self.load_data_to_graphdb(blob)

    # sync files only newly uploaded
    def partial_sync(self, blobs):
        for blob in blobs:
           if blob.content_type == 'application/json' and blob.time_created > self.last_upd_sync:
               self.load_data_to_graphdb(blob)