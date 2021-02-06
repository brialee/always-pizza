import pandas as PD
import matplotlib.pyplot as plt

# Read data from CSV
crash_data = PD.read_csv('OR-HWY26-2019-crash-data.csv')

# Check Assertion: Every record has a CRASH_ID.
missing_crash_id = False
for record in crash_data['Crash ID']:
    if str(record) == 'nan':
        missing_crash_id = True
        break

print("\n------- Assertion 1.1")
if missing_crash_id:
    print("Not all records have a Crash ID.")
else:
    print("All records have a Crash ID.")

# Check assertion: Every record has a field or set of fields that can be used to compose the crash date.
# Fields in question are 'Crash Month', 'Crash Day', 'Crash Year'. If a record is missing the 'Crash Month'
# and there is no way we can accurately determine the date of the crash.
missing_crash_date_field = False
for record in crash_data['Crash Month']:
    if str(record) == 'nan':
        missing_crash_date_field = True
        break

print("\n------- Assertion 1.2")
if missing_crash_date_field:
    print("Not all records have a Crash Month.")
else:
    print("All records have a Crash Month.")


# Check assertion: The HWY_NO field will be no more than 3 characters.
long_hwy_value = False
for record in crash_data['Highway Number']:
    # Note the value is a float
    try:
        n = len(str(int(record)))
        if n > 3:
            long_hwy_value = True
            break
    except ValueError as e:
        #print("Cannot convert to int " + str(record))
        pass

print("\n------- Assertion 2.1")
if long_hwy_value:
    print("Some Highway Numbers are more then three characters.")
else:
    print("No Highway Numbers are more than three characters.")


# Check assertion: The AGE_VAL will be a two character field. 
two_char_age = True
for record in crash_data['Age']:
    # Note the value is a float
    try:
        n = len(str(int(record)))
        if n < 2:
            two_char_age = False
            break
    except ValueError as e:
        pass

print("\n------- Assertion 3.1")
if two_char_age:
    print("All age values are two digits.")
else:
    print("Some age values are less than two digits.")


# Check Assertion: The VHCL_OCCUP_CNT will be > 0.
one_occupant = True
for record in crash_data['Vehicle Occupant Count']:
    # Note the value is a float
    try:
        if int(record) == 0:
            one_occupant = False
            break
    except ValueError as e:
        pass

print("\n------- Assertion 3.2")
if one_occupant:
    print("All vehicles had at least one occupant.")
else:
    print("Some vehicles had zero number of occupants.")


# Check Assertion: Every CRASH_ID has an associated SER_NO.
assoc_serial_no = True
assoc_df = crash_data[['Crash ID', 'Serial #']]

for entry in assoc_df.values:
    crash_id, serialno = entry
    try:
        cid = int(crash_id)
        sno = int(serialno)
    # Empty values appear as 'nan'
    except ValueError as e:
        assoc_serial_no = False
        break

print("\n------- Assertion 4.1")
if assoc_serial_no:
    print("All crash ids have an associated serial number.")
else:
    print("Not all crash ids have an assocaited serial number.")


# Check assertion: Every VHCL_ID is associated with one or more CRASH_IDs
# Do we have a case where there is a VHCL_ID, but not CRASH_ID?
assoc_vehicle_id = True
assoc_df = crash_data[['Crash ID', 'Vehicle ID']]

# Shortcut, if we know all records have a crash id, then
# it stands to reason that no vehicle id would be without
# a crash id
print("\n------- Assertion 4.2")
if not missing_crash_id:
    print("All vehicle ids have an associated crash id (shortcut)")
else:
    for entry in assoc_df:
        crash_id, vehicle_id = entry
        has_cid = False
        has_vid = False

        try:
            int(crash_id)
            has_cid = True
        except:
            pass

        try:
            int(vehicle_id)
            has_vid = True
        except:
            pass
    
        # We have a vehicle id without a crash id
        if has_vid and not has_cid:
            assoc_vehicle_id = False
            break

    if not assoc_vehicle_id:
        print("Not all vehicle ids have an associated crash id")
    else:
        print("All vehicle ids have an associated crash id")


# Check assertion: Every record has a unique VHCL_ID value
print("\n------- Assertion 5.1")
vids = crash_data['Vehicle ID']
vids_unique = []
is_vids_unique = True
for entry in vids:
    try:
        vid = int(entry)
        if vid not in vids_unique:
            vids_unique.append(int(entry))
        else:
            print("Not all Vehcile IDs are unique")
            is_vids_unique = False
            break
    except:
        pass
if is_vids_unique:
    print("All Vehicle IDs are unqique")


# Check assertion: Every record has a CMPSS_DIR_FROM_CD which is one of N|S|E|W
print("\n------- Assertion 5.2")
vdir_codes = crash_data['Vehicle Travel Direction From']
empty_count = 0
for entry in vdir_codes:
    try:
        if entry.lower() not in ['n', 's', 'e', 'w']:
            print("Not all Vehicle Travel Direction From values correspond to a cardinal direction")
    except:
        empty_count += 1
if empty_count == len(vdir_codes):
    print("The Vehicle Travel Direction From field is empty for all rows.")
else:
    print("All Vehicle Travel Direction From values correspond to a cardinal direction")


# Check assertion: Crashes are less prevalent during the middle of the week (e.g. Tue-Thur)
print("\n------- Assertion 7.1")
print(crash_data['Week Day Code'].value_counts())
# plt.show()
print("This isn't quite true.....")


# Check assertion: The distribution of crashes will be skewed towards the end/beginning of the calendar year.
print("\n------- Assertion 7.2")
cd = crash_data.sort_values(by=['Crash Month'], ascending=False)
print(cd['Crash Month'].value_counts())
print("The distribution skews more towards the beginning of the year.")


# Remove the vehicle travel destination from column as the records are all empty
# NOTE this will break previous test(s) that use this column
crash_data.drop('Vehicle Travel Direction From', axis=1, inplace=True)