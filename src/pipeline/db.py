import warnings; warnings.simplefilter(action='ignore', category=UserWarning)
import psycopg2
import pandas as pd
from typing import Dict
from contextlib import contextmanager

from utils.logger import logger

class PostgreSQL:
    def __init__(self, db_name: str, user: str, password: str, host: str, port: str) -> None:
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to the database successfully.")
        except Exception as e:
            raise Exception(f"Error connecting to the {self.db_name} database: {e}")

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        Commits if no exceptions occur, rolls back otherwise.
        """
        try:
            yield
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction failed, rolling back. Error: {e}")
            raise
    
    def prepare_tuples(self, df: pd.DataFrame) -> list:
        """Convert a DataFrame to a list of tuples."""
        return list(df.itertuples(index=False, name=None))

    def close(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed.")
    
    ### --- REPORT TABLE OPERATIONS --- ###

    def create_report_table(self) -> None:
        """Create the macrotrends_pe_pb_hist table if it does not exist."""
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS macrotrends_pe_pb_hist (
            Symbol VARCHAR(10),
            Name VARCHAR(100),
            Date DATE,
            Stock_Price FLOAT,
            Book_Value_per_Share FLOAT,
            Price_to_Book_Ratio FLOAT,
            TTM_Net_EPS FLOAT, 
            PE_Ratio FLOAT,
            CONSTRAINT unique_symbol_date UNIQUE (Symbol, Date)
        );
        '''
        try:
            with self.transaction():
                self.cursor.execute(create_table_query)
            logger.info(f"macrotrends_pe_pb_hist table created successfully.")
        except Exception as e:
            raise Exception(f"Error creating macrotrends_pe_pb_hist table: {e}")

    def store_report_dataframes(self, dataframes: list[pd.DataFrame]) -> None:
        """Store multiple DataFrames in the PostgreSQL table."""
        insert_query = '''
        INSERT INTO macrotrends_pe_pb_hist (Date, Symbol, Name, Stock_Price, Book_Value_per_Share, Price_to_Book_Ratio, TTM_Net_EPS, PE_Ratio) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (Symbol, Date) DO NOTHING
        '''
        
        all_data = []
        for df in dataframes:
            all_data.extend(self.prepare_tuples(df))
        
        with self.transaction():
            self.cursor.executemany(insert_query, all_data)
            logger.info(f"Sent {len(all_data)} rows to 'macrotrends_pe_pb_hist' table.")

    def load_report_dataframe(self, symbol: str = None) -> pd.DataFrame:
        """Load data from the PostgreSQL table."""
        if symbol is None:
            query = 'SELECT * FROM macrotrends_pe_pb_hist;'
        else:
            query = 'SELECT * FROM macrotrends_pe_pb_hist WHERE Symbol = %s;'
            
        try:
            df = pd.read_sql(query, self.conn, params=(symbol,))
            return df
        except Exception as e:
            logger.error(f"Error loading data from the macrotrends_pe_pb_hist table: {e}")
            return pd.DataFrame()
        
    ### --- CURRENT RATIOS TABLE OPERATIONS --- ###
    
    def create_current_ratio_table(self) -> None:
        """Create the current_ratios table if it does not exist."""
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS current_ratios (
            Symbol VARCHAR(10),
            Last_Update TIMESTAMP,
            PB_Ratio FLOAT,
            PE_Ratio FLOAT,
            CONSTRAINT unique_symbol UNIQUE (Symbol)
        );
        '''
        try:
            with self.transaction():
                self.cursor.execute(create_table_query)
            logger.info("Table created successfully.")
        except Exception as e:
            raise Exception(f"Error creating current_ratios table: {e}")

    def store_current_ratio_dataframes(self, dataframes: list[pd.DataFrame]) -> None:
        """Store multiple DataFrames in the PostgreSQL table."""
        insert_query = '''
        INSERT INTO current_ratios (Symbol, Last_Update, PB_Ratio, PE_Ratio) 
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (Symbol) 
        DO UPDATE SET
            PB_Ratio = EXCLUDED.PB_Ratio,
            PE_Ratio = EXCLUDED.PE_Ratio,
            Last_Update = EXCLUDED.Last_Update;
        '''
        all_data = []
        for df in dataframes:
            all_data.extend(self.prepare_tuples(df))
        
        with self.transaction():
            self.cursor.executemany(insert_query, all_data)
            logger.info(f"Sent {len(all_data)} rows to 'current_ratios' table.")

    def load_current_ratio_dataframe(self, symbol: str = None) -> pd.DataFrame:
        """Load data from the PostgreSQL table."""
        if symbol is None:
            query = 'SELECT * FROM current_ratios;'
        else:
            query = 'SELECT * FROM current_ratios WHERE Symbol = %s;'
            
        try:
            df = pd.read_sql(query, self.conn, params=(symbol,))
            return df
        except Exception as e:
            logger.error(f"Error loading data from current_ratios table: {e}")
            return pd.DataFrame()
        
    def __enter__(self):
        # Context handler enter
        self.connect()
        self.create_report_table() # Create the tables if they do not already exist
        self.create_current_ratio_table()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        # Context handler exit
        self.close()