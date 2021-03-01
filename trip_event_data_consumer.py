from confluent_kafka import Consumer
import json
from datetime import datetime
import ccloud_lib
import psycopg2 as PG
import psycopg2.extras
import dbconfig as DB


def send_to_db(event_data):
    conn = PG.connect(
        host=DB.host,
        database=DB.database,
        user=DB.user,
        password=DB.password,)
    conn.autocommit=True

    total_updates = 0
    cur = conn.cursor()
    for record in event_data:
        statement = "update trip set route_id=%s, service_key='%s', direction='%s' "
        statement += "where trip_id=" + str(record['trip_id'])
        query = statement % (record['route_id'], record['service_key'], record['direction'])
        cur.execute(query)
        total_updates+=1

    cur.close()
    print("DB UPDATES: " + str(total_updates))

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'project-1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    count = 0
    message_data = []
    msg = True
    try:
        print("consuming data.....")
        while msg is not None:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                message_data.append(data)
                count =+ 1
                total_count += count
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    # Pass list of consumed messages to DB for insertion
    print("Sending records to db")
    send_to_db(message_data)
    print("Record updates: " + str(total_count))
