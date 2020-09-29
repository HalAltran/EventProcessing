import argparse
import json

import boto3

from botocore.config import Config


class EventProcessing:

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("aws_public")
        parser.add_argument("aws_secret")
        # parser.add_argument('-bucket_name', type=str)
        args = parser.parse_args()
        # my_config = Config()
        # print(my_config)
        self.session = boto3.Session(
            aws_access_key_id=args.aws_public,
            aws_secret_access_key=args.aws_secret
        )
        #
        s3 = self.session.resource('s3')
        bucket = s3.Bucket('eventprocessing-altran-locationss3bucket-1ub1fsm0jlky7')

        # s3_obj = s3.Object('locations.json', 'locations.json').get()
        # bucket.d

        # file_as_string = bucket.get_object(Key='locations.json')['Body'].read().decode('utf-8')

        # print(file_as_string)

        bucket.download_file('locations.json', 'locations.json')


        # file_string = None
        # with open('locations.json', 'r') as location_file:
        #     file_string = json.loads(location_file.read())
        #
        # print(file_string)
