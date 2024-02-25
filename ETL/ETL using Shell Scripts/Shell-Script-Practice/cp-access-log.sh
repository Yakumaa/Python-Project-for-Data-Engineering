# cp-access-log.sh
# This script downloads the file 'web-server-access-log.txt.gz'
# from "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/".

# The script then extracts the .txt file using gunzip.

# The .txt file contains the timestamp, latitude, longitude 
# and visitor id apart from other data.

# Transforms the text delimeter from "#" to "," and saves to a csv file.
# Loads the data from the CSV file into the table 'access_log' in PostgreSQL database.

#Extract phase
echo "Extracting data"

#Extract timestamp, latitude, longitude and visitorid which are the first four fields from the file using the cut command.
cut -d"#" -f1-4 web-server-access-log.txt > extracted-data.txt

# Transform phase
echo "Transforming data"

# read the extracted data and replace the hashtags with commas.
tr "#" "," < extracted-data.txt > transformed-data.csv

# Load phase
echo "Loading data"

# Send the instructions to connect to 'template1' and
# copy the file to the table 'access_log' through command pipeline.
echo "\c template1;\COPY access_log  FROM '/mnt/d/python/Python Project for Data Engineering/ETL/ETL using Shell Scripts/Shell-Script-Practice/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost
