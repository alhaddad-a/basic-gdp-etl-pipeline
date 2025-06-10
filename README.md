# Countries by GDP (Nominal) – ETL Pipeline Project

This project extracts GDP (nominal) data from a Wikipedia snapshot, transforms it into a clean format, loads it into a CSV file and SQLite database, and visualizes the top 10 economies by GDP.

---

## Project Overview

- **Data Source:** [Archived Wikipedia Page (Sept 2023)](https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29)
- **Tech Stack:** Python, Pandas, BeautifulSoup, Matplotlib, SQLite
- **Output:** Cleaned GDP data as CSV + SQLite DB + Top 10 GDP Bar Chart

---

## Features

- Web scraping using `requests` and `BeautifulSoup`
- Data cleaning (removing "—", commas, and converting millions → billions)
- Transformation to float values, rounded to 2 decimal places
- Export to CSV and SQLite database
- Visualize top 10 countries by GDP
- Logs every step to a text file (`log_file.txt`)

---

## Requirements

Install the dependencies with:

```bash
pip install -r requirements.txt
```

---

## Project Structure

```text
final-project/
├── etl_project_gdp.py         # Main ETL script
├── requirements.txt           # Python dependencies
├── Countries_by_GDP.csv       # Output CSV file
├── World_Economies.db         # SQLite DB (created after running)
├── top_gdp_chart.png          # Saved chart (bar graph)
├── log_file.txt               # ETL process log
└── README.md                  # This file
```

---

## How to Run

1. Clone the repo or copy the files into a folder
2. Make sure Python 3.x is installed
3. Run the script:

```bash
python etl_project_gdp.py
```

---

## Output Example

```
SELECT * FROM Countries_by_GDP WHERE GDP_USD_billions >= 100
         Country       GDP_USD_billions
0   United States            26854.60
1           China            19373.59
2           Japan             4409.74
...
```

And the chart will be saved as `top_gdp_chart.png`.
