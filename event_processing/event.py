from datetime import datetime
import event_processing.main as main


class Event:

    def __init__(self, event_dict):
        self.location_id = event_dict['locationId']
        self.event_id = event_dict['eventId']
        self.value = event_dict['value']
        timestamp_to_seconds = int(event_dict['timestamp'] / 1000)
        self.timestamp = datetime.utcfromtimestamp(timestamp_to_seconds + 3600)
        self.time_rounded_to_minute = main.EventProcessing.round_time_to_nearest_min(timestamp_to_seconds)
