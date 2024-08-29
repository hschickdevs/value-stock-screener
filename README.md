# Value Stock Screener

[**Technical Overview**](#technical-overview) | [**Screener Criteria**](#screener-criteria) | [**Installation & Setup Instructions**](#installation--setup-instructions) | [**Demo**](#demo)

This software scrapes fundamental stock data from Macrotrends (like PE & PB ratios), stores it in a PostgreSQL database, and then offers a web interface to filter and view the data based on a set of screening criteria to identify potential value stocks.

The original intent of this software was to support my own investing efforts, however I decided to open-source the software to share my work with other retail value investors who may find it useful. 

## Technical Overview

<img src="src/.streamlit/logos/macrotrends-logo.png" alt="Value Stock Screener" width="60"/>
<img src="src/.streamlit/logos/docker-logo.png" alt="Value Stock Screener" width="60"/>
<img src="src/.streamlit/logos/postgresql-logo.png" alt="Value Stock Screener" width="60"/>
<img src="src/.streamlit/logos/python-logo.png" alt="Value Stock Screener" width="60"/>
<img src="src/.streamlit/logos/streamlit-logo.png" alt="Value Stock Screener" width="60"/>

[Password protected](./src/.streamlit/secrets.toml) Streamlit web interface offering screener results and further screening criteria configuration options

Background scraper process pulling data from [Macrotrends](https://www.macrotrends.net/) across 4500+ stocks listed across the NYSE and NASDAQ exchanges

Lightweight PostgreSQL database to storing the historical and current fundamental stock data

Simple orchestration using Docker compose

## Screener Criteria:

While the criteria _can be adjusted_ in the dashboard, the default values are as follows:

* Exclude preferred stocks, rights, warrants, or ticker symbols with 5 letters or more 

* At least 7 years of positive P/B ratio history

* Current P/B ratio is less than 2

* Current price-to-book (P/B) ratio is less than the lower of either the 3-year average P/B ratio or the 7+ year average P/B ratio, multiplied by the margin of safety factor.

## Installation & Setup Instructions

### 1. PostgreSQL Database Setup

#### Install and start the database:

1. Install Docker on your machine.

2. Pull the PostgreSQL Docker image:

    ```bash
    docker pull postgres
    ```

3. Run the PostgreSQL container:

    ```bash
    docker run --name sample_postgres_db -e POSTGRES_PASSWORD=sample_password -e POSTGRES_USER=sample_user -e POSTGRES_DB=sample_database -p 5432:5432 -d postgres
    ```

4. Open the port `5432` on your machine to allow connections to the PostgreSQL database.

5. Enter the PostgreSQL shell

    ```bash
    docker exec -it sample_postgres_db psql -U sample_user -d sample_database
    ```

    Query to get # rows and cols:

    ```
    WITH row_count AS (
        SELECT COUNT(*) AS num_rows 
        FROM macrotrends_pe_pb_hist
    ), col_count AS (
        SELECT COUNT(*) AS num_cols 
        FROM information_schema.columns 
        WHERE table_name = 'macrotrends_pe_pb_hist'
    )
    SELECT 
        row_count.num_rows, 
        col_count.num_cols 
    FROM 
        row_count, col_count;
    ```

### 2. Run the Data Polling Script

1. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

2. Run the data polling script:

    ```bash
    python3 src/Poller.py
    ```

This process will take quite a few hours to complete, but the Streamlit dashboard is still able to run while it is processing.

### 3. Run the Streamlit Dashboard

1. CD into the project directory:

    ```bash
    cd src/
    ```

2. Run the Streamlit dashboard:

    ```bash
    streamlit run Home.py
    ```

## Demo

![Value Stock Screener Demo](src/.streamlit/logos/demo.gif)