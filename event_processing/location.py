from datetime import datetime


class Location:

    def __init__(self, location_dict):
        self.x = location_dict['x']
        self.y = location_dict['y']
        self.id = location_dict['id']

        self.events = []

        self.latest_average_value = 0
        self.latest_event_count = 0
        self.average_value_at_time_dict = {}

    def update_average_values_at_time(self, time_to_calculate):
        self.latest_event_count = 0
        sum_of_values = 0
        for event in self.events:
            if event.time_rounded_to_minute == time_to_calculate:
                # remove event from self.events
                # remove event id from event_id_set in main
                sum_of_values += event.value
                self.latest_event_count += 1
        self.latest_average_value = 0
        if self.latest_event_count > 0:
            self.latest_average_value = sum_of_values / self.latest_event_count

        formatted_time = datetime.strftime(datetime.utcfromtimestamp(time_to_calculate + 3600), "%d/%m/%Y %H:%M:%S")
        self.average_value_at_time_dict[formatted_time] = self.latest_average_value
