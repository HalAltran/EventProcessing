import argparse
import json

import boto3

LOCATION_FILE_NAME = 'locations.json'


class EventProcessing:

    def __init__(self):
        params = self.parse_input_args()
        self.aws_id = params.aws_public
        self.aws_key = params.aws_secret
        self.bucket_name = params.bucket_name

        self.bucket = self.get_bucket()

        self.locations_list = []

    def run(self):
        self.bucket.download_file(LOCATION_FILE_NAME, LOCATION_FILE_NAME)

        self.populate_locations_dict()

        sqs_client = boto3.client('sqs')

        create_queue_response = sqs_client.create_queue(QueueName='first_queue')

        queue_url = create_queue_response['QueueUrl']

        sqs_client.delete_queue(QueueUrl=queue_url)

    def populate_locations_dict(self):
        with open(LOCATION_FILE_NAME, 'r') as location_file:
            self.locations_list.extend(json.loads(location_file.read()))

    def get_bucket(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key
        )
        return session.resource('s3').Bucket(self.bucket_name)

    @staticmethod
    def parse_input_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("aws_public")
        parser.add_argument("aws_secret")
        parser.add_argument("bucket_name")
        return parser.parse_args()
