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
        self.sns_topic_name = params.sns_topic_name

        self.bucket = self.get_bucket()

        self.locations_list = []

    def run(self):
        self.bucket.download_file(LOCATION_FILE_NAME, LOCATION_FILE_NAME)

        self.populate_locations_dict()

        # set up sqs client and create queue

        sqs_client = boto3.client('sqs', region_name='eu-west-1')

        create_queue_response = sqs_client.create_queue(QueueName='first_queue')
        queue_url = create_queue_response['QueueUrl']
        queue_arn = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']

        # add permissions

        policy_document = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': f'allow-subscription-{self.sns_topic_name}',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': f'{queue_arn}',
                'Condition': {
                    'ArnEquals': {'aws:SourceArn': f'{self.sns_topic_name}'}
                }
            }]
        }

        policy_json = json.dumps(policy_document)

        sns_client = boto3.client('sns', region_name='eu-west-1')

        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={'Policy': policy_json}
        )

        subscribe_request = sns_client.subscribe(
            TopicArn=self.sns_topic_name,
            Protocol='sqs',
            Endpoint=queue_arn,
            ReturnSubscriptionArn=True
        )

        print(sqs_client.receive_message(QueueUrl=queue_url))

        sns_client.unsubscribe(SubscriptionArn=subscribe_request['SubscriptionArn'])

        sqs_client.delete_queue(QueueUrl=queue_url)

    def populate_locations_dict(self):
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
