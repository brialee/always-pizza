import urllib.request
import json
from datetime import datetime

src_uri = "http://rbi.ddns.net/getBreadCrumbData"



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
            with open(fname, 'w') as outFile:
            json.dump(json_resp, outFile)
        except Exception as e:
            print("Failed to save data to file " + filename)
            return None
        else:
            return json_resp



if __name__ == "__main__":
    # Current year-month-date.json 
    # datetime.now().strftime('%Y-%d-%m') + ".json"

    
