import json
import boto3
import event_processing.main as main


class SQSClientWrapper:

    def __init__(self, sns_topic_name):
        self.sns_topic_name = sns_topic_name
        self.client = boto3.client('sqs', region_name=main.REGION_NAME)

        create_queue_response = self.client.create_queue(QueueName='first_queue')
        self.queue_url = create_queue_response['QueueUrl']
        self.queue_arn = self.client.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']

        self.client.set_queue_attributes(
            QueueUrl=self.queue_url,
            Attributes={'Policy': self.get_policy_json()}
        )

    def get_policy_json(self):
        return json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': f'allow-subscription-{self.sns_topic_name}',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': f'{self.queue_arn}',
                'Condition': {
                    'ArnEquals': {'aws:SourceArn': f'{self.sns_topic_name}'}
                }
            }]
        })

    def receive_message(self, max_number_of_messages):
        return self.client.receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=5,
                                           MaxNumberOfMessages=max_number_of_messages)

    def create_delete_queue_request(self):
        self.client.delete_queue(QueueUrl=self.queue_url)
