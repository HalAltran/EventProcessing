import boto3
import event_processing.main as main


class SNSClientWrapper:

    def __init__(self, sns_topic_name):
        self.sns_topic_name = sns_topic_name
        self.client = boto3.client('sns', region_name=main.REGION_NAME)
        self.subscription_arn = None

    def create_subscribe_request(self, sqs_client_wrapper):
        subscribe_request = self.client.subscribe(
            TopicArn=self.sns_topic_name,
            Protocol='sqs',
            Endpoint=sqs_client_wrapper.queue_arn,
            ReturnSubscriptionArn=True
        )
        self.subscription_arn = subscribe_request['SubscriptionArn']

    def create_unsubscribe_request(self):
        self.client.unsubscribe(SubscriptionArn=self.subscription_arn)
