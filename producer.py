import urllib.request
import json
from datetime import datetime

import ccloud_lib
from confluent_kafka import Producer, KafkaError

src_uri = "http://rbi.ddns.net/getBreadCrumbData"
delivered_records = 0


# Return sensor data read from json file
def sensor_data_from_file(filename):
    try:
        sensor_json = None
        with open(filename, 'r') as inFile:
            sensor_json = json.load(inFile)
    except Exception as e:
        print("Failed to read sensor data from " + filename)
        return None

    if not sensor_json or sensor_json == '':
        print("No sensor data to read from " + filename)
    else:
        return sensor_json


# Return sensor data retrieved from remote source.
# Pass a filename to save data as JSON.
def sensor_data_from_uri(uri, filename=None):
    try:
        response = urllib.request.urlopen(uri)
        json_resp = json.loads(response.read())
    except Exception as e:
        print("Failed to get data from " + uri)
        return None
    
    if not filename:
        return json_resp
    else:
        try:
            with open(filename, 'w') as outFile:
                json.dump(json_resp, outFile)
        except Exception as e:
            print("Failed to save data to file " + filename)
            return None
        else:
            return json_resp

# Publish one or more records to the topic
# read from 
def publish_records(producer, records, record_key):
    for record in records[94507:]:
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



if __name__ == "__main__":
    # Current year-month-date.json 
    # datetime.now().strftime('%Y-%d-%m') + ".json"

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


    json_data = sensor_data_from_uri(src_uri)
    if json_data:
        publish_records(producer, json_data, 'sensor-data-record')
    
