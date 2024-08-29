import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
from pipeline.db import PostgreSQL
from tabulate import tabulate
import numpy as np
from math import ceil

from utils.misc import check_password

def calculate_avg_pb_ratios(stock_data: pd.DataFrame) -> tuple[float, float, int]:
    """
    :param stock_data: DataFrame containing the individual stock data
    
    :return: A tuple containing the 3-year average P/B ratio and the max (7-10) year average P/B ratio, and the available years
    """
    stock_data.sort_values('date', inplace=True)
    
    # Calculate the date three years back from the most recent date
    last_q_report_date = stock_data['date'].max()
    available_years = (last_q_report_date.year - stock_data['date'].min().year) + 1
    
    # Calculate the 3-year average p/b ratio
    date_three_years_ago = last_q_report_date - timedelta(days=3*365)
    avg_calc_rows = stock_data[stock_data['date'] > date_three_years_ago]
    avg_pb_3_year = avg_calc_rows['price_to_book_ratio'].mean()
    
    # Calculate the average P/B ratio for the entire available period (this would already be filtered to 7-10 years)
    avg_pb_max_year = stock_data['price_to_book_ratio'].mean()
    
    return avg_pb_3_year, avg_pb_max_year, available_years


@st.cache_data
def screen_stocks(df, current_df, criteria) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Screen stocks based on the following criteria:
    * >= 7 years of positive P/B ratio history
    * Current P/B ratio is less than the specified value and not equal to 0
    * Current price-to-book (P/B) ratio is less than the lower of either the 3-year average P/B ratio
      or the 7+ year average P/B ratio, multiplied by the margin of safety factor.
    """    
    # Convert the date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    
    # Remove rows from the current year
    df = df[df['year'] != datetime.now().year]
    
    # Determine the most recent year in the filtered dataset
    most_recent_year = df['year'].max()
    
    # Remove any data that is older than 10 years from the most recent year
    df = df[df['year'] > (most_recent_year - 10)]
    
    def stock_meets_criteria(group) -> bool:
        """
        Has to be inside the body of this function to get the criteria variable.
        """        
        valid = True
        
        try:
            current_ratios_row = current_df[current_df['symbol'] == group['symbol'].iloc[0]].iloc[0]
            use_current_ratios = True
        except Exception as e:
            # print(f"WARNING: Error fetching current ratios for {group['symbol'].iloc[0]}:", e)
            use_current_ratios = False
        
        # Check if the stock has at least x years of P/B history
        if group['year'].nunique() < criteria['years_pb_history']:
            valid = False
        
        # Ensure all P/B ratios in the period are positive
        if criteria['only_positive_pb']:
            pb_ratios = group['price_to_book_ratio']
            if (pb_ratios <= 0).any():
                valid = False
            
        # Get the latest P/B ratio and check if it meets the current P/B ratio criteria
        latest_pb_ratio = current_ratios_row['pb_ratio'] if use_current_ratios else group.sort_values('date').iloc[-1]['price_to_book_ratio']
        if latest_pb_ratio >= criteria['max_current_pb_ratio']:
            valid = False
        
        # Check if the latest P/B ratio is less than the safety threshold
        avg_pb_3_year, avg_pb_max_year, _ = calculate_avg_pb_ratios(group)        
        if latest_pb_ratio > (criteria['pb_margin_of_safety'] * min(avg_pb_3_year, avg_pb_max_year)):
            valid = False
        
        # Ensure that the stock has at least x years of positive P/E history
        group.sort_values('date', inplace=True, ascending=False)
        pe_ratios = group.groupby('year').first()['pe_ratio']  
        if (pe_ratios > 0).sum() < criteria['years_positive_pe_history']:
            valid = False
        
        # Get the latest P/E ratio and check if it meets the current P/E ratio criteria
        latest_pe_ratio = current_ratios_row['pe_ratio'] if use_current_ratios else group.sort_values('date').iloc[-1]['pe_ratio']
        if latest_pe_ratio >= criteria['max_current_pe_ratio']:
            valid = False
        
        return valid
    
    # Group by symbol and apply the screening criteria
    screened_symbols = df.groupby('symbol').filter(stock_meets_criteria)['symbol'].unique()
    
    # Filter the original DataFrame to include only the screened symbols
    filtered_df = df[df['symbol'].isin(screened_symbols)]
    
    filtered_current_df = current_df[current_df['symbol'].isin(screened_symbols)]
    
    return filtered_df, filtered_current_df

def prepare_screener_results_preview(df, criteria) -> pd.DataFrame:
    df = df.sort_values(by='date', ascending=False)
    df = df.groupby('symbol').first().reset_index()
    df.drop(columns=['year'], inplace=True)
    df = df.rename(columns={
        'name': 'company_name',
        'date': 'last_quarterly_report_date',
        'stock_price': 'stock_price',
        'book_value_per_share': 'bv_per_share',
        'price_to_book_ratio': 'pb_ratio',
        'ttm_net_eps': 'earnings_per_share',
    })
    
    return df

def prepare_individual_stock_report(df, current_df, symbol) -> tuple[pd.DataFrame, pd.DataFrame]:
    stock_data = df[df['symbol'] == symbol]
    stock_data = stock_data.sort_values('date')
    most_recent_date = stock_data['date'].max()
    avg_pb_3_year, avg_pb_max_year, available_years = calculate_avg_pb_ratios(stock_data)
    
    try:
        current_ratios_row = current_df[current_df['symbol'] == symbol].iloc[0]
        latest_pb_ratio = current_ratios_row['pb_ratio']
        latest_pe_ratio = current_ratios_row['pe_ratio']
    except:
        latest_pb_ratio = stock_data[stock_data['date'] == most_recent_date]['price_to_book_ratio'].iloc[0]
        latest_pe_ratio = stock_data[stock_data['date'] == most_recent_date]['pe_ratio'].iloc[0]
        
    lower_avg_pb = min(avg_pb_3_year, avg_pb_max_year)
    ratio = latest_pb_ratio / lower_avg_pb if lower_avg_pb != 0 else None

    fundamentals_report_dict = {
        'Current P/E Ratio': latest_pe_ratio,
        'Current P/B Ratio': latest_pb_ratio,
        '3 Year Average P/B': avg_pb_3_year,
        f'{available_years} Year Average P/B': avg_pb_max_year,
        'Relative P/B Ratio': ratio,
        'Past 5 Year EPS Growth Rate': None,
        'Next 5 Year EPS Growth Rate': None,
        'Beta Value': None,
        'Debt Equity': None,
        '7 Year Average EPS': None,
        '7 Year Average PE': None,
        'Intrinsic Value Using Earnings': None,
        'Book Value': None,
        'Intrinsic Value Based on Book Value': None,
        'Projected EPS': None
    }
    fundamentals_report = pd.DataFrame(list(fundamentals_report_dict.items()), columns=['metric_name', 'metric_value'])

    stock_data['year'] = stock_data['date'].dt.year
    time_series_report = stock_data.groupby('year').last().reset_index()
    time_series_report.rename(columns={'date': 'report_date'}, inplace=True)
    time_series_report.drop(columns=['name', 'year'], inplace=True)
    
    return time_series_report, fundamentals_report

def download_stock_report_txt(symbol: str, company_name: str, time_series_data: pd.DataFrame, fundamental_metrics: pd.DataFrame):
    report_text = ""
    report_text += f"{pd.Timestamp.now().date()} {company_name} ({symbol}) Stock Report\n\n"
    report_text += f"Symbol: {symbol}\n"
    report_text += f"Company_Name: {company_name}\n\n"
    time_series_data['report_date'] = pd.to_datetime(time_series_data['report_date'])
    time_series_data['report_date'] = time_series_data['report_date'].dt.strftime('%Y-%m')
    
    col_renames = {
        'report_date': 'P/B Reporting Year',
        'ttm_net_eps': 'Earnings Per Share (USD)',
        'stock_price': 'Stock Price',
        'book_value_per_share': 'Book Value per Share (USD)',
        'pe_ratio': 'P/E Ratio',
        'price_to_book_ratio': 'P/B Ratio'
    }
    time_series_data.rename(columns=col_renames, inplace=True)
    time_series_data.drop(columns=['symbol'], inplace=True)
    
    report_text += tabulate(time_series_data, headers='keys', tablefmt='pretty', showindex=False)
    report_text += "\n\n"
    
    fundamental_metrics = fundamental_metrics.fillna('')
    report_text += tabulate(fundamental_metrics, headers='keys', tablefmt='pretty', showindex=False)
    
    return report_text


# Function to load data from PostgreSQL and cache the connection for 30 minutes 
@st.cache_data(ttl=1800)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    # Connect to the PostgreSQL database
    db = PostgreSQL(db_name='sample_database', user='sample_user', password='sample_password', host='172.105.101.61', port='5432')
    db.connect()
    
    ratio_history_df = db.load_report_dataframe()
    ratio_history_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    current_ratio_df = db.load_current_ratio_dataframe()
    current_ratio_df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
    
    db.close()
    
    return ratio_history_df, current_ratio_df


# Main function to render the screener page
def main():
    if not check_password():
        st.stop()  # Do not continue if check_password is not True.
    
    st.markdown("## Value Stock Screener")
    
    # Load data from PostgreSQL
    historic_data, current_data = load_data()
        
    ## Screener configuration inputs
    
    st.sidebar.header("Price-to-Book Settings")
    
    years_pb_history = st.sidebar.number_input("Minimum Years of P/B History", min_value=1, value=7, max_value=10)
    only_positive_pb = st.sidebar.checkbox("Only Positive P/B History", value=True)
    current_pb_ratio = st.sidebar.number_input("Maximum Current P/B Ratio", value=2.0, max_value=current_data['pb_ratio'].max())
    pb_margin_of_safety = st.sidebar.number_input("Margin of Safety Factor", value=1.0)
    
    st.sidebar.header("Price-to-Earnings Settings")
    
    max_pe = ceil(current_data['pe_ratio'].max())
    years_positive_pe_history = st.sidebar.number_input("Minimum Years Positive of P/E History", min_value=1, value=7, max_value=10)
    current_pe_ratio = st.sidebar.number_input("Maximum Current P/E Ratio", value=max_pe, max_value=max_pe)

    # Apply screening criteria
    criteria = {
        "years_pb_history": years_pb_history,
        "max_current_pb_ratio": current_pb_ratio,
        "only_positive_pb": only_positive_pb,
        "pb_margin_of_safety": pb_margin_of_safety,
        
        # "years_pe_history": years_pe_history,
        "max_current_pe_ratio": current_pe_ratio,
        # "only_positive_pe": only_positive_pe,
        "years_positive_pe_history": years_positive_pe_history,
        "pe_margin_of_safety": 1.0,  # Placeholder for future use
    }
    # with st.spinner(f"Screening {len(current_data)} stocks, please wait ..."):
    screener_results, screener_current_results = screen_stocks(historic_data, current_data, criteria)
    
    st.write("👈 Configure the screener in the left-hand sidebar.")
    
    st.markdown(f"#### {len(screener_results['symbol'].unique())} Symbols in Screener Results:")
    
    # Display the filtered stocks
    st.dataframe(prepare_screener_results_preview(screener_results, criteria), hide_index=True, use_container_width=True)
    
    # Create a selectbox for stock selection below the screener results
    selected_symbol = st.selectbox('Select a screened stock symbol to view individual report:', screener_results['symbol'].unique())
    selected_name = screener_results[screener_results['symbol'] == selected_symbol]['name'].iloc[0]
    
    # Generate and display the report for the selected stock as a dataframe
    if selected_symbol:
        time_series_report, fundamental_report = prepare_individual_stock_report(screener_results, screener_current_results, selected_symbol)
        st.markdown(f"### {selected_name} ({selected_symbol}) Stock Report")
        
        # col1, col2 = st.columns([0.6, 0.4])
        # with col1:            
        st.markdown(f"#### P/B Time Series Data:")
        
        gb = GridOptionsBuilder.from_dataframe(time_series_report)
        gb.configure_columns(column_names=['ttm_net_eps', 'pe_ratio'], editable=True)
        go = gb.build()
        time_series_ag = AgGrid(
            time_series_report, 
            gridOptions=go, 
            # height=400, 
            fit_columns_on_grid_load=True
        )
        
        st.download_button(label=f"Download {selected_symbol} Time Series Data",
                            data=time_series_ag['data'].to_csv(index=False).encode("utf-8"),
                            file_name=f"{selected_symbol}_time_series_report.csv",
                            mime="text/csv")
        
        # with col2:
        st.markdown(f"#### Fundamental Metrics:")
        
        gb = GridOptionsBuilder.from_dataframe(fundamental_report)
        gb.configure_columns(column_names=list(fundamental_report.columns), editable=True)
        go = gb.build()
        fundamentals_ag = AgGrid(
            fundamental_report, 
            gridOptions=go, 
            # height=400, 
            fit_columns_on_grid_load=True
        )
        
        st.download_button(label=f"Download Full {selected_symbol} Stock Report",
                              data=download_stock_report_txt(selected_symbol, selected_name, time_series_ag['data'], fundamentals_ag['data']),
                              file_name=f"{selected_symbol}_stock_report.txt",
                              mime="text/plain")
            

if __name__ == "__main__":
    main()
