# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime

# Defining the URL and the table attributes
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'ETL/World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'ETL/Countries_by_GDP.csv'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    # Send a GET request to the specified URL
    html_page = requests.get(url).text
    # Create a BeautifulSoup object
    data = BeautifulSoup(html_page, 'html.parser')
    # Find the table containing the required data
    tables = data.find_all('tbody')
    # Extract the required data from the table
    rows = tables[2].find_all('tr')
    # Create an empty dataframe to hold the extracted data
    df = pd.DataFrame(columns=table_attribs)
    # Iterate through the rows of the table and extract the data
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
              data_dict = {
                  "Country": col[0].text,
                  "GDP_USD_millions": col[2].contents[0]
              }
              df1 = pd.DataFrame(data_dict, index=[0])
              df = pd.concat([df, df1], ignore_index=True)
    return df
    

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    # Remove the commas from the GDP values and convert to float
    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '').astype(float)
    
    df['GDP_USD_millions'] = round(df['GDP_USD_millions'] / 1000, 2)

    df.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'}, inplace=True)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open('ETL/GDP_log_file.txt',"a") as f:
        f.write(timestamp + ',' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("ETL Job Started")

log_progress("Extracting Data")
extracted_data = extract(url, table_attribs)
print(extracted_data)

log_progress("Transforming Data")
transformed_data = transform(extracted_data)
print(transformed_data)

log_progress("Loading Data")
load_to_csv(transformed_data, csv_path)
print('Data saved to CSV')

log_progress("Loading Data to Database")
conn = sqlite3.connect(db_name)
load_to_db(transformed_data, conn, table_name)

log_progress("Running Query")
query_statement = f"SELECT * FROM {table_name} where GDP_USD_billions >= 100;"
print(query_statement)
run_query(query_statement, conn)

log_progress("ETL Job Completed")

conn.close()

