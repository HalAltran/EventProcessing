class Event:

    def __init__(self, event_dict):
        self.location_id = event_dict['locationId']
        self.event_id = event_dict['eventId']
        self.value = event_dict['value']
        self.timestamp = event_dict['timestamp']
