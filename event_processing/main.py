import argparse
import csv
import json
import pprint
import boto3
import time

from datetime import timedelta
from datetime import datetime
from event_processing.event import Event
from event_processing.location import Location
from event_processing.sns_client_wrapper import SNSClientWrapper
from event_processing.sqs_client_wrapper import SQSClientWrapper

LOCATION_FILE_NAME = 'locations-part2.json'
REGION_NAME = 'eu-west-1'
RUN_TIME = 60 * 24
TIME_TO_WAIT_FOR_EVENTS = 60 * 5


class EventProcessing:

    def __init__(self):
        params = self.parse_input_args()
        self.aws_id = params.aws_public
        self.aws_key = params.aws_secret
        self.bucket_name = params.bucket_name
        self.sns_topic_name = params.sns_topic_name

        self.bucket = self.get_bucket()

        self.locations = {}

        self.sqs_client_wrapper = SQSClientWrapper(self.sns_topic_name)
        self.sns_client_wrapper = SNSClientWrapper(self.sns_topic_name)

        self.event_id_set = set()

    def run(self):
        self.bucket.download_file(LOCATION_FILE_NAME, LOCATION_FILE_NAME)

        self.populate_locations_list()

        self.sns_client_wrapper.create_subscribe_request(self.sqs_client_wrapper)

        self.process_messages()

        self.sns_client_wrapper.create_unsubscribe_request()
        self.sqs_client_wrapper.create_delete_queue_request()

        self.write_event_data_to_file()

    def process_messages(self):

        end_time = time.time() + RUN_TIME
        time_of_printing = self.round_time_to_nearest_min(time.time()) + 60

        while time.time() < end_time:
            receive_messages_response = self.sqs_client_wrapper.receive_message(10)

            if 'Messages' in receive_messages_response:
                messages = receive_messages_response['Messages']

                for message in messages:
                    try:
                        self.add_event_to_corresponding_location(message)
                    except:
                        pass

                if self.round_time_to_nearest_min(time.time()) >= time_of_printing:
                    self.print_average_values(time_of_printing)
                    time_of_printing = self.round_time_to_nearest_min(time.time()) + 60

                self.sqs_client_wrapper.delete_received_messages(messages)

    def add_event_to_corresponding_location(self, message):
        message_body = json.loads(message['Body'])
        inner_message = json.loads(message_body['Message'])
        event_id = inner_message['eventId']
        location_id = inner_message['locationId']
        if location_id in self.locations and event_id not in self.event_id_set:
            self.event_id_set.add(event_id)
            self.locations[location_id].events.append(Event(inner_message))

    def print_average_values(self, time_of_printing):
        time_to_calculate = time_of_printing - TIME_TO_WAIT_FOR_EVENTS

        sum_of_values = 0
        value_count = 0
        for location in self.locations.values():
            location.update_average_values_at_time(time_to_calculate)
            sum_of_values += location.latest_event_count * location.latest_average_value
            value_count += location.latest_event_count

        current_time = datetime.utcfromtimestamp(time_to_calculate + 3600)
        avg_value = 0
        if value_count != 0:
            avg_value = sum_of_values / value_count
        queue_size = self.sqs_client_wrapper.get_queue_size()
        print("time: %s; average value: %f; number of values: %i; queue size: %s" %
              (datetime.strftime(current_time, "%d/%m/%Y %H:%M:%S"), avg_value, value_count, queue_size))

    def populate_locations_list(self):
        locations_data = []
        with open(LOCATION_FILE_NAME, 'r') as location_file:
            locations_data.extend(json.loads(location_file.read()))
        for location_dict in locations_data:
            location = Location(location_dict)
            self.locations[location.id] = location

    def get_bucket(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key,
            region_name=REGION_NAME
        )
        return session.resource('s3').Bucket(self.bucket_name)

    def write_event_data_to_file(self):
        cols = []
        for location in self.locations.values():

            entries_1 = [location.id, location.x, 'time']
            entries_2 = ['', location.y, 'average value']

            for rounded_time, average_value in location.average_value_at_time_dict.items():
                entries_1.append(rounded_time)
                entries_2.append(average_value)

            cols.append(entries_1)
            cols.append(entries_2)

        # rows = []
        rows = zip(*cols)
        # for row in rows:
        #     print(row)

        # for col in cols:
        #     print(col)
        # pprint.pp(cols)
        with open('output.csv', 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=',')
            for row in rows:
                writer.writerow(row)

    @staticmethod
    def parse_input_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("aws_public")
        parser.add_argument("aws_secret")
        parser.add_argument("bucket_name")
        parser.add_argument("sns_topic_name")
        return parser.parse_args()

    @staticmethod
    def round_time_to_nearest_min(input_time):
        return int(input_time - input_time % 60)

    # @staticmethod
    # def round_time(dt=None, round_to=60):
    #     if dt is None:
    #         dt = datetime.now()
    #     seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    #     rounding = (seconds + round_to / 2) // round_to * round_to
    #     return dt + timedelta(0, rounding - seconds, -dt.microsecond)
