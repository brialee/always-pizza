import json

'''
Transform existing files into a single list of objects with the event trip id
as part of said object. This way it conforms to the formatting from the 
sensor data JSON.

Also update values to conform to:
service_type: Weekday, Saturday, Sunday ( service_key )
tripdir_type: Out, Back ( direction )

* really all that we need are k,v pairs for:
- trip id
- route id
- vehicle id
- service key
- direction
* as we're only updating the trip table
'''

data = None
transformed = []
with open('2020-10-18-trip-event-data.json', 'r') as inFile:
    data = json.load(inFile)

if not data:
    print("nothing to work with")
    exit()
else:
    for trip_id, vals in data.items():
        for entry in vals:
            slim_data = {}
            slim_data['trip_id'] = trip_id
            slim_data['route_id'] = entry['route_number']
            slim_data['vehicle_id'] = entry['vehicle_number']

            # Convert to In/Out
            if entry['direction'] == "1":
                slim_data['direction'] = "Out"
            else:
                slim_data['direction'] = "In"

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


with open("2020-10-18-event-data.json", 'w') as outFile:
    json.dump(transformed, outFile)