import argparse
import csv
import json
import boto3
import time
import matplotlib.pyplot as plt

from mpl_toolkits.mplot3d import Axes3D
from datetime import datetime
from event_processing.event import Event
from event_processing.location import Location
from event_processing.potential_source import PotentialSource
from event_processing.sns_client_wrapper import SNSClientWrapper
from event_processing.sqs_client_wrapper import SQSClientWrapper

LOCATION_FILE_NAME = 'locations-part2.json'
REGION_NAME = 'eu-west-1'
RUN_TIME = 60 * 40
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

        self.estimate_source_location()

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

        rows = zip(*cols)

        with open('output.csv', 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=',')
            for row in rows:
                writer.writerow(row)

    def estimate_source_location(self):
        location_list = self.locations.values()
        x_min, x_max, y_min, y_max = 0, 0, 0, 0
        for location in location_list:
            if location.x < x_min:
                x_min = location.x
            if location.x > x_max:
                x_max = location.x
            if location.y < y_min:
                y_min = location.y
            if location.y > y_max:
                y_max = location.y

        x_step = (x_max - x_min) / 100
        y_step = (y_max - y_min) / 100
        x = x_min
        # y = y_min

        potential_sources = []
        while x < x_max:
            y = y_min
            while y < y_max:

                sum_of_inverse_squared_distances = 0
                for location in location_list:
                    inverse_distance_squared = 1 / ((x - location.x) ** 2 + (y - location.y) ** 2)
                    sum_of_inverse_squared_distances += inverse_distance_squared

                expected_value = 0
                for location in location_list:
                    location.update_overall_average_value()
                    inverse_distance_squared = 1 / ((x - location.x) ** 2 + (y - location.y) ** 2)
                    expected_value += location.overall_average_value * inverse_distance_squared / sum_of_inverse_squared_distances

                potential_sources.append(PotentialSource(x, y, expected_value))

                y += y_step
            x += x_step

        potential_sources.sort(key=lambda potential_source: potential_source.value, reverse=True)
        expected_source = potential_sources[0]
        print('x: %f, y: %f, value: %f' % (expected_source.x, expected_source.y, expected_source.value))
        self.plot_graph(potential_sources)

    def plot_graph(self, potential_sources):
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')

        x_values = []
        y_values = []
        z_values = []

        for location in self.locations.values():
            x_values.append(location.x)
            y_values.append(location.y)
            z_values.append(location.overall_average_value)

        p_x = []
        p_y = []
        p_z = []
        for potential_source in potential_sources:
            p_x.append(potential_source.x)
            p_y.append(potential_source.y)
            p_z.append(potential_source.value)

        ax.scatter(x_values, y_values, z_values, color='b')
        # ax.scatter(p_x, p_y, p_z, color='r')

        plt.show()

        print("Displaying graph")

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
