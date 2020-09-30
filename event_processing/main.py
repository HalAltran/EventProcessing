import argparse
import json
import pprint
import boto3

from event_processing.event import Event
from event_processing.location import Location
from event_processing.sns_client_wrapper import SNSClientWrapper
from event_processing.sqs_client_wrapper import SQSClientWrapper

LOCATION_FILE_NAME = 'locations.json'
REGION_NAME = 'eu-west-1'


class EventProcessing:

    def __init__(self):
        params = self.parse_input_args()
        self.aws_id = params.aws_public
        self.aws_key = params.aws_secret
        self.bucket_name = params.bucket_name
        self.sns_topic_name = params.sns_topic_name

        self.bucket = self.get_bucket()

        # self.locations_list = []
        self.locations = {}

        self.sqs_client_wrapper = SQSClientWrapper(self.sns_topic_name)
        self.sns_client_wrapper = SNSClientWrapper(self.sns_topic_name)

        self.event_id_set = set()

    def run(self):
        self.bucket.download_file(LOCATION_FILE_NAME, LOCATION_FILE_NAME)

        self.populate_locations_list()

        self.sns_client_wrapper.create_subscribe_request(self.sqs_client_wrapper)

        self.process_messages()

        for location in self.locations.values():
            print('location id: %s' % location.id)
            for event in location.events:
                print('event value: %f, event timestamp: %i' % (event.value, event.timestamp))
        # pprint.pp(self.sqs_client_wrapper.receive_message())

        self.sns_client_wrapper.create_unsubscribe_request()
        self.sqs_client_wrapper.create_delete_queue_request()

    def process_messages(self):
        for i in range(0, 5):
            messages = self.sqs_client_wrapper.receive_message(10)['Messages']

            for message in messages:
                message_body = json.loads(message['Body'])
                # message_id = message_body['MessageId']  # event id?
                inner_message = json.loads(message_body['Message'])
                event_id = inner_message['eventId']
                location_id = inner_message['locationId']
                if location_id in self.locations and event_id not in self.event_id_set:
                    self.event_id_set.add(event_id)
                    self.locations[location_id].events.append(Event(inner_message))

            # delete message batch. need receipt handle from message body?

    def populate_locations_list(self):
        locations_data = []
        with open(LOCATION_FILE_NAME, 'r') as location_file:
            locations_data.extend(json.loads(location_file.read()))
        for location_dict in locations_data:
            location = Location(location_dict)
            self.locations[location.id] = location
            # self.locations_list.append(Location(location_dict))

    def get_bucket(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key,
            region_name=REGION_NAME
        )
        return session.resource('s3').Bucket(self.bucket_name)

    @staticmethod
    def parse_input_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("aws_public")
        parser.add_argument("aws_secret")
        parser.add_argument("bucket_name")
        parser.add_argument("sns_topic_name")
        return parser.parse_args()
