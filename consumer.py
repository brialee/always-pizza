
from confluent_kafka import Consumer
import json
from datetime import datetime
import ccloud_lib
import psycopg2 as PG
import psycopg2.extras
import dbconfig as DB



#### Validation cases
def validate_time(data):
    if data == "":
        return "0"
    if int(data) < 0:
        return "0"
    if int(data) > 86400:
        return "0"
    return data

def validate_lat(data):
    if data == "":
        return "0"
    if float(data) < -90:
        return "0"
    if float(data) > 90:
        return "0"
    return data

def validate_long(data):
    if data == "":
        return "0"
    if float(data) < -180:
        return "0"
    if float(data) > 180:
        return "0"
    return data

def validate_direction(data):
    if data == "":
        return "-1"
    if int(data) < 0:
        return "-1"
    if int(data) > 359:
        return "-1"
    return data

def validate_if_empty_get_zero(data):
    if data == "":
        return "0"
    else:
        return data


def validate_data(sensor_data):
    for trip_id, recs in sensor_data.items():
        for rec in recs:
            rec['ACT_TIME'] = validate_time(rec['ACT_TIME'])
            rec['GPS_HDOP'] = validate_if_empty_get_zero(rec['GPS_HDOP'])
            rec['SCHEDULE_DEVIATION'] = validate_if_empty_get_zero(rec['SCHEDULE_DEVIATION'])
            rec['METERS'] = validate_if_empty_get_zero(rec['METERS'])
            rec['RADIO_QUALITY'] = validate_if_empty_get_zero(rec['RADIO_QUALITY'])
            rec['GPS_SATELLITES'] = validate_if_empty_get_zero(rec['GPS_SATELLITES'])


# Get appropriate timestamp for record
# Dates are in the format day-three letter month-two digit year
# time is total number of second elapsee since midnight
def get_timestamp(datestr, timestr):
    d = datetime.strptime(datestr, '%d-%b-%y')
    epoch_time = d.timestamp() + float(timestr)
    act_date = datetime.fromtimestamp(epoch_time)
    return act_date.strftime('%d-%b-%y %H:%m:%S')

# event_no_trip : associated records
def dict_from_sensor_data(sensor_data):
    sensor_dict = {}
    for entry in sensor_data:
        try:
            sensor_dict[entry['EVENT_NO_TRIP']].append(entry)
        except KeyError:
            sensor_dict[entry['EVENT_NO_TRIP']] = []
            sensor_dict[entry['EVENT_NO_TRIP']].append(entry)
    return sensor_dict


# Returns 
# ( trip_table_data = [(..,..,)], breadcrumb_table_data = [(..,..,)] )
def data_tuples_from_dict(sensor_dict):
    trip_table_data = []
    breadcrumb_table_data = []
    for trip_id, recs in sensor_dict.items():
        trip_vals = (trip_id, recs[0]['VEHICLE_ID'])
        trip_table_data.append(trip_vals)

        crumbs = []
        for rec in recs:
            longitude = validate_long(rec['GPS_LONGITUDE'])
            lat = validate_lat(rec['GPS_LATITUDE'])
            direction = validate_direction(rec['DIRECTION'])
            velocity = validate_if_empty_get_zero(rec['VELOCITY'])

            crumb = (
                    get_timestamp(rec['OPD_DATE'],rec['ACT_TIME']),
                    lat,
                    longitude,
                    direction,
                    velocity,
                    trip_id)
            crumbs.append(crumb)
        breadcrumb_table_data += crumbs

    return trip_table_data, breadcrumb_table_data


#
def send_to_db(trip_table_data, breadcrumb_table_data):
    conn = PG.connect(
        host=DB.host,
        database=DB.database,
        user=DB.user,
        password=DB.password,)
    conn.autocommit=True

    cur = conn.cursor()
    statement = "insert into trip (trip_id, vehicle_id) VALUES (%s, %s);"
    try:
        psycopg2.extras.execute_batch(cur, statement, trip_table_data)
        print("Trip table updated: " + str(len(trip_table_data)))
    except Exception as e:
        print("Exception in Trip batch insert: " + str(e))

    statement = "insert into breadcrumb values (%s,%s,%s,%s,%s,%s);"
    try:
        psycopg2.extras.execute_batch(cur, statement, breadcrumb_table_data)
        print("Breadcrumb table updated: " + str(len(breadcrumb_table_data)))
    except Exception as e:
        print("Exception in breadcrumb batch insert: " + str(e))
    cur.close()


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
    failed_poll = 0
    message_data = []
    msg = True
    try:
        print("consuming data.....")
        while failed_poll < 30:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll(): " + str(failed_poll))
                failed_poll += 1
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
                failed_poll += 1
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                message_data.append(data)
                count =+ 1
                total_count += count
                failed_poll = 0
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    print("creating dict")
    data_dict = dict_from_sensor_data(message_data)
    validate_data(data_dict)
    print("parsing dict")
    trip, bread = data_tuples_from_dict(data_dict)
    print("Sending records to db")
    send_to_db(trip, bread)
