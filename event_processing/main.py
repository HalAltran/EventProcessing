import argparse
import json
import pprint
import boto3

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

        self.locations_list = []

        self.sqs_client_wrapper = SQSClientWrapper(self.sns_topic_name)
        self.sns_client_wrapper = SNSClientWrapper(self.sns_topic_name)

    def run(self):
        self.bucket.download_file(LOCATION_FILE_NAME, LOCATION_FILE_NAME)

        self.populate_locations_list()

        self.sns_client_wrapper.create_subscribe_request(self.sqs_client_wrapper)

        pprint.pp(self.sqs_client_wrapper.receive_message())

        self.sns_client_wrapper.create_unsubscribe_request()

        self.sqs_client_wrapper.create_delete_queue_request()

    def populate_locations_list(self):
        with open(LOCATION_FILE_NAME, 'r') as location_file:
            self.locations_list.extend(json.loads(location_file.read()))

    def get_bucket(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key,
            region_name='eu-west-1'
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
