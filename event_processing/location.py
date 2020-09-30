class Location:

    def __init__(self, location_dict):
        self.x = location_dict['x']
        self.y = location_dict['y']
        self.id = location_dict['id']

        self.events = []
