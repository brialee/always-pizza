import pandas as PD
import matplotlib.pyplot as plt

# Read data from CSV
crash_data = PD.read_csv('OR-HWY26-2019-crash-data.csv')

# Check assertion: No 'crash' type records have a vehicle id.
no_vehicle_ids = True
print("\nAssertion 1.1 >> No 'crash' type records have a vehicle id.", end=" >> ")
for record in crash_data[['Record Type', 'Vehicle ID']].values:
    rec_type, vid = record
    try:
        if int(rec_type) == 1 and int(vid):
            no_vehicle_ids = False
            break
    # NaN?
    except ValueError as e:
        pass

print(no_vehicle_ids)


# Check assertion: All 'crash' type records have serial numbers.
print("\nAssertion 1.2 >> All 'crash' type records have serial numbers.", end=" >> ")
all_have_serial = True
for record in crash_data[['Record Type', 'Serial #']].values:
    rec_type, serialno = record
    try:
        if int(rec_type) == 1:
            n = int(serialno)
    # Could not cast serialno into int ( assumes all rec_types are present )
    except ValueError as e:
        all_have_serial = False
        break
print(all_have_serial)


# Check assertion: The Crash Month value ( when present ) will be between 1-12
print("\nAssertion 2.1 >> The Crash Month value ( when present ) will be between 1-12.", end=" >> ")
valid_crash_mo = True
for record in crash_data['Crash Month']:
    try:
        mo_no = int(record)
        if mo_no > 12 or mo_no < 1:
            valid_crash_mo = False
            break
    # Wasn't a number
    except ValueError as e:
        pass
print(valid_crash_mo)

# Check assertion: If the 'marijuana used' field is available it will be between 0-3 || 9.
print("\nAssertion 2.2 >> If the 'marijuana used' field is available it will be between 0-3 || 9", end=" >> ")
good_weed = True
good_values = [0,1,2,3,9]
for record in crash_data['Crash Marijuana Involved Flag']:
    try:
        n = int(record)
        if n not in good_values:
            good_weed = False
            break
    except ValueError as e:
        pass
print(good_weed)

# Check assertion: The alcohol involved flag will either be blank | 0 | 1 | 9
print("\nAssertion 3.1 >> The alcohol involved flag will either be blank | 0 | 1 | 9", end=" >> ")
good_drinks = True
good_values = [0,1,9]
for record in crash_data['Alcohol-Involved Flag']:
    try:
        n = int(record)
        if n not in good_values:
            good_drinks = False
            break
    except ValueError as e:
        pass
print(good_drinks)


# Check assertion: Vehicle records will have a hit & run flag value of either 0 | 1
print("\nAssertion 3.2 >> Vehicle records will have a hit & run flag value of either 0 | 1", end=" >> ")
valid_run_flag = True
for record in crash_data[['Record Type', 'Vehicle Hit & Run Flag']].values:
    rec_type, flag_val = record
    try:
        if int(rec_type) == 2:
            if int(flag_val) != 0 or int(flag_val) != 1:
                run_flag_fail = False
                break
    except ValueError as e:
        pass
print(valid_run_flag)


# Check assertion: All the participant records will have a participant ID
print("\nAssertion 4.1 >> All the participant records will have a participant ID", end=" >> ")
valid_pid = True
for record in crash_data[['Record Type', 'Participant ID']].values:
    rec_type, pid = record
    try:
        if int(rec_type) == 3:
            n = int(pid)
    except ValueError as e:
        valid_pid = False
        break
print(valid_pid)


# Check assertion: All the crash records will have a light condition value between 0-5
print("\nAssertion 4.2 >> All the crash records will have a light condition value between 0-5", end=" >> ")
see_the_light = True
for record in crash_data[['Record Type', 'Light Condition']].values:
    rec_type, light_con = record
    try:
        if int(rec_type) == 1:
           if int(light_con) > 5 or int(light_con) < 0:
               see_the_light = False
               break 
    except ValueError as e:
        pass
print(see_the_light)


# Check assertion: Every crash record has a weather condition value between 0-9
print("\nAssertion 5.1 >> Every crash record has a weather condition value between 0-9", end=" >> ")
nice_weather = True
for record in crash_data[['Record Type', 'Weather Condition']].values:
    rec_type, wcon = record
    try:
        if int(rec_type) == 1:
            if int(wcon) > 9 or int(wcon) < 0:
                nice_weather = False
                break
    except ValueError as e:
        pass
print(nice_weather)


# Check assertion: Every vehicle record has a vehicle ownership value between 0-9
print("\nAssertion 5.2 >> Every vehicle record has a vehicle ownership value between 0-9", end=" >> ")
has_owner = True
for record in crash_data[['Record Type', 'Vehicle Ownership Code']].values:
    rec_type, owner_code = record
    try:
        if int(rec_type) == 2:
            if int(owner_code) > 9 or int(owner_code) < 0:
                has_owner = False
                break
    except ValueError as e:
        pass
print(has_owner)


# Check assertion: Every crash record where the roundabout value is 1, will also have a off roadway value 1
print("\nAssertion 6.1 >> Every crash record where the roundabout value is 1, will also have a off roadway value 1", end=" >> ")
consistent_flag = True
for record in crash_data[['Record Type', 'Roundabout Flag', 'Off Roadway Flag']].values:
    rec_type, round_flag, road_flag = record
    try:
        if int(rec_type) == 1:
            if int(round_flag) == 1 and int(road_flag) != 1:
                consistent_flag = False
                break
    except ValueError as e:
        pass
print(consistent_flag)


# Check assertion: Every crash record that has an intersection related value of 1, will also have an intersection type value ( non-blank )
print("\nAssertion 6.2 >> Every crash record that has an intersection related value of 1, will also have an intersection type value ( non-blank )", end=" >> ")
valid_intersection = True
for record in crash_data[['Record Type', 'Intersection Type', 'Intersection Related Flag']].values:
    rec_type, intersection_type, intersection_flag = record
    try:
        if int(rec_type) == 1:
            if int(intersection_flag) == 1:
                try:
                    x = int(intersection_type)
                except ValueError as e:
                    valid_intersection = False
                    break
    except ValueError as e:
        pass
print(valid_intersection)

# Check assertion: Light conditions for crashes will skew towards non-daylight ( 2-3 )
print("\nAssertion 7.1 >> Light conditions for crashes will skew towards non-daylight ( 2-3 )")
print(crash_data['Light Condition'].value_counts())
print("Allegedly, every crash in Oregon during 2019 happended during the daytime...")


# Check assertion: Road surface conditions for crashes will skew towards non-dry surfaces ( 2-4 )
print("\nAssertion 7.2 >> Road surface conditions for crashes will skew towards non-dry surfaces ( 2-4 )")
print(crash_data['Road Surface Condition'].value_counts())
print("The most frequent value is once not listed in the CDS manual.")


 # Remove the light condition column as all the values are the same
 # NOTE this would break previous test(s) that use this column
crash_data.drop('Light Condition', axis=1, inplace=True)