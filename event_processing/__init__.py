from event_processing.main import EventProcessing

if __name__ == "__main__":
    processing = EventProcessing()
    processing.run()
    print(processing.locations_list)
