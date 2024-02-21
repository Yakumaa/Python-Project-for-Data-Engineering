# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime

# Defining the URL and the table attributes
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'ETL/Bank Project/Banks.db'
table_name = 'Largest_banks'
csv_path = 'ETL/Bank Project/Largest_banks_data.csv'

def extract(url, table_attribs):
    # Send a GET request to the specified URL
    html_page = requests.get(url).text
    # Create a BeautifulSoup object
    data = BeautifulSoup(html_page, 'html.parser')
    # Find the table containing the required data
    tables = data.find_all('tbody')
    # Extract the required data from the table
    rows = tables[0].find_all('tr')
    # Create an empty dataframe to hold the extracted data
    df = pd.DataFrame(columns=table_attribs)
    # Iterate through the rows of the table and extract the data
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                "Name": col[1].text.strip(),
                "MC_USD_Billion": col[2].contents[0].strip()
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df
    

def transform(df):
    # Remove the commas from the GDP values and convert to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '').astype(float)

    #Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR,
    #rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
    exchange_rate = pd.read_csv('ETL/Bank Project/Exchange_Rate.csv')
    exchange_rate = exchange_rate.set_index('Currency')

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rate.loc['GBP', 'Rate'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate.loc['EUR', 'Rate'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rate.loc['INR', 'Rate'], 2)

    print(df['MC_EUR_Billion'][4])
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
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open('ETL/Bank Project/code_log.txt',"a") as f:
        f.write(timestamp + ':' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")

extracted_data = extract(url, table_attribs)
print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data = transform(extracted_data)
print(transformed_data)
log_progress("Data transformation complete. Initiating Loading process")

log_progress("Loading Data")
load_to_csv(transformed_data, csv_path)
print('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(transformed_data, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

log_progress("Running Query")
query_statement = f"SELECT * FROM {table_name};"
print(query_statement)
run_query(query_statement, conn)

log_progress("Running Query")
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks;"
print(query_statement)
run_query(query_statement, conn)

log_progress("Running Query")
query_statement = f"SELECT Name from Largest_banks LIMIT 5;"
print(query_statement)
run_query(query_statement, conn)

log_progress("Process Complete")

conn.close()
log_progress("Server Connection closed")

