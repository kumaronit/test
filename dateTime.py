from datetime import datetime

# input timestamp
timestamp_string = '2023-03-07 01:00:00'

# Converting timestamp string to datetime object and formatting it
datetime_object = datetime.fromtimestamp(timestamp_string).strftime('%d-%m-%y')

# printing resultant datetime object
print("Resultant datetime object:",datetime_object)

# printing the type of resultant datetime object
print("Type of datetime object:", type(datetime_object))