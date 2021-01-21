
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib



if __name__ == '__main__':

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

    delivered_records = 0

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



    # Send over sample data, one record at a time
    try:
        sample_data = None
        with open('sample_1k.json') as inFile:
            sample_data = json.load(inFile)

    except Exception as e:
        print("Failed to open sample data source: " + str(e))
        exit()


    for record in sample_data:
        record_key = "sensor data"
        record_value = json.dumps(record)
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)


    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))

