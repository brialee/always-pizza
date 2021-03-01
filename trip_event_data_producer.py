from bs4 import BeautifulSoup
from urllib.request import urlopen
import json
import re

import ccloud_lib
from confluent_kafka import Producer, KafkaError

stops_data_url = "http://34.83.136.192:8000/getStopEvents/"
html = urlopen(stops_data_url)
stop_data_soup = BeautifulSoup(html, 'lxml')
delivered_records = 0

# Date for data as a single item
stop_data_date = stop_data_soup.find('h1').contents[0].strip()

# The actual date ( for saving file as date.json )
stop_date_text = stop_data_date.split("for ")[1]

# All tables are preceeded by an H3 tag
events_by_trip = stop_data_soup.find_all('h3')


# Publish one or more records to the topic
# read from 
def publish_records(producer, records, record_key):
    for record in records:
        record_key = record_key
        record_value = json.dumps(record)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(.01)

        # global delivered_records
        # if delivered_records % 50 == 0:
        #     producer.flush()

    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))


# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))


'''
Construct dict of stop data
{
    'tripid':[
        { k,v pairs for single event },
        { ... },
    ]
}
'''
def event_data_to_dict(events_by_trip):
    stop_data = {}
    for trip_num in events_by_trip:
        # Get the actual trip num from the string.
        # This will be the key for all relevant stop data
        trip_no = re.findall(r'[0-9]+', trip_num.contents[0])
        stop_data[trip_no[0]] = []

        # Find the nearest table, that will have the data for
        # the associated trip id
        data_table = trip_num.find_next('table')
        headers = data_table.find_all('th')
        values = data_table.find_all('tr')

        header_list = []
        for header in headers:
            #print(header.contents[0])
            header_list.append(header.contents[0])

        # First <tr> will be the headers
        value_list = []
        for value in values[1:]:
            data = value.find_all('td')
            for d in data:
                try:
                    value_list.append(d.contents[0])
                except IndexError:
                    value_list.append(None)

            # construct k,v pairs for trip id key
            agg_data = {}
            idx = 0
            while idx < len(header_list):
                agg_data[header_list[idx]] = value_list[idx]
                idx+=1

            stop_data[trip_no[0]].append(agg_data)
    return stop_data



'''
Transform dict into:
[
    {
        "trip_id": "170571488",
        "route_id": "25",
        "vehicle_id": "2286",
        "direction": "In",
        "service_key": "Saturday"
    },
    { ... }
]

'''
def transform_data(stop_data):
    transformed = []
    if not stop_data:
        print("nothing to work with")
        return None
    else:
        for trip_id, vals in stop_data.items():
            for entry in vals:
                slim_data = {}
                slim_data['trip_id'] = trip_id
                slim_data['route_id'] = entry['route_number']
                slim_data['vehicle_id'] = entry['vehicle_number']

                # Convert to In/Out
                if entry['direction'] == "0":
                    slim_data['direction'] = "Out"
                else:
                    slim_data['direction'] = "Back"

                # Convert to Weekday,Saturday,Sunday
                if entry['service_key'] == "W":
                    slim_data['service_key'] = "Weekday"
                elif entry['service_key'] == "S":
                    slim_data['service_key'] = "Saturday"
                elif entry['service_key'] == "U":
                    slim_data['service_key'] = "Sunday"
                else:
                    slim_data['service_key'] = None

                transformed.append(slim_data)

    # with open(stop_date_text + "-event-data.json", 'w') as outFile:
    #     json.dump(transformed, outFile)
    return transformed



# Return event data read from json file
def event_data_from_file(filename):
    try:
        event_json = None
        with open(filename, 'r') as inFile:
            event_json = json.load(inFile)
    except Exception as e:
        print("Failed to read event data from " + filename)
        return None

    if not event_json or event_json == '':
        print("No event data to read from " + filename)
    else:
        return event_json



if __name__ == "__main__":
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    # HTML table to dict
    # d = event_data_to_dict(events_by_trip)
    # event_data = transform_data(d)

    event_data = event_data_from_file('2020-10-18-event-data.json')

    if event_data:
        publish_records(producer, event_data, 'event-data-record')
