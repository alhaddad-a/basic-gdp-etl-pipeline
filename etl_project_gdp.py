# === IMPORTS ===
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests
import sqlite3
import matplotlib.pyplot as plt

# === CONFIGURATION ===
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
csv_path = r'C:\Users\adenh\Desktop\gdp-etl-pipeline\Countries_by_GDP.csv'
log_file = "log_file.txt"
table_name = 'Countries_by_GDP'
sql_connection = sqlite3.connect('World_Economies.db')

# === LOGGING FUNCTIONS ===
def log_progress(message, level="INFO"):
    """Log a message with a timestamp and level (INFO or ERROR)."""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp}, {level}, {message}\n")

# === DATA EXTRACTION ===
def extract(url, table_attribs):
    """Extract GDP data from the Wikipedia page and return a DataFrame."""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            imf_estimate_cell = cols[2]
            country_cell = cols[0]
            if country_cell.find('a') and imf_estimate_cell.get_text(strip=True) != '—':
                country = country_cell.get_text(strip=True)
                imf_estimate = imf_estimate_cell.get_text(strip=True)
                data_dict = {'Country': country, 'GDP_USD_millions': imf_estimate}
                df1 = pd.DataFrame([data_dict])
                df = pd.concat([df, df1], ignore_index=True)

    return df

# === DATA TRANSFORMATION ===
def transform(data):
    """Convert GDP from string format to float, and from millions to billions."""
    data['GDP_USD_millions'] = data['GDP_USD_millions'].str.replace(',', '', regex=False)
    data['GDP_USD_millions'] = data['GDP_USD_millions'].astype(float)
    data['GDP_USD_millions'] = (data['GDP_USD_millions'] / 1000).round(2)
    data.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'}, inplace=True)
    return data

# === LOAD TO CSV / DB ===
def load_to_csv(data, csv_path):
    """Save DataFrame to CSV."""
    data.to_csv(csv_path, index=False)

def load_to_db(data, sql_connection, table_name):
    """Save DataFrame to SQLite database."""
    data.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """Run a SQL SELECT query and display the result."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# === VISUALIZATION ===
def visualize_top_gdp(data, top_n=10, save_path='top_gdp_chart.png'):
    """Generate and save a bar chart of the top N countries by GDP."""
    top_countries = data.sort_values(by='GDP_USD_billions', ascending=False).head(top_n)

    plt.figure(figsize=(12, 6))
    plt.bar(top_countries['Country'], top_countries['GDP_USD_billions'])
    plt.xlabel('Country')
    plt.ylabel('GDP (in USD Billions)')
    plt.title(f'Top {top_n} Countries by GDP')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(save_path)
    print(f"Chart saved as: {save_path}")
    try:
        plt.show()
    except:
        pass

# === ETL PIPELINE EXECUTION ===
log_progress('Preliminaries complete. Initiating ETL process')

# 1. Extract
try:
    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')
except Exception as e:
    log_progress(f"Error during extraction: {str(e)}", level="ERROR")
    raise

# 2. Transform
try:
    df = transform(df)
    log_progress('Data transformation complete. Initiating loading process')
except Exception as e:
    log_progress(f"Error during transformation: {str(e)}", level="ERROR")
    raise

# 3. Load to CSV
try:
    load_to_csv(df, csv_path)
    log_progress('Data saved to CSV file')
except Exception as e:
    log_progress(f"Error saving to CSV: {str(e)}", level="ERROR")
    raise

# 4. Load to Database
try:
    log_progress('SQL Connection initiated.')
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table.')
except Exception as e:
    log_progress(f"Error loading to database: {str(e)}", level="ERROR")
    raise

# 5. Query
try:
    query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
    log_progress('Running the query')
    run_query(query_statement, sql_connection)
    log_progress('Query run successfully')
except Exception as e:
    log_progress(f"Error running query: {str(e)}", level="ERROR")
    raise

# Close DB connection
sql_connection.close()
log_progress('SQL Connection closed.')

# 6. Visualization
try:
    visualize_top_gdp(df, top_n=10)
    log_progress('Visualization complete.')
except Exception as e:
    log_progress(f"Error during visualization: {str(e)}", level="ERROR")

log_progress('ETL process complete.')
