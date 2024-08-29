import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from yahoo_fin.stock_info import get_live_price

from utils.misc import check_password

# Initialize session state for DataFrame
if 'df' not in st.session_state:
    st.session_state.df = None

# Initialize cache for current prices
if 'price_cache' not in st.session_state:
    st.session_state.price_cache = {}

# Function to check target prices
def check_target_prices(df):
    current_prices = st.session_state.price_cache
    for _, r in df.iterrows():
        ticker = r['Ticker']
        if len(str(ticker)) == 0 or ticker is None:
            pass
        elif ticker not in current_prices:
            current_prices[ticker] = get_live_price(ticker)

    df["Current Price"] = df["Ticker"].apply(lambda x: current_prices.get(x, 0) if current_prices.get(x) is not None else 0)
    df["Reached Target"] = df["Current Price"] >= df["Target Price"]
    return df[df["Reached Target"]]

# Function to classify stocks by holding period
def classify_by_holding_period(df):
    today = datetime.today()
    one_year_ago = today - timedelta(days=365)
    two_years_ago = today - timedelta(days=730)
    three_years_ago = today - timedelta(days=1095)
    
    # Make sure that the Purchase Date column is a datetime
    df["Purchase Date"] = pd.to_datetime(df["Purchase Date"])

    one_year_df = df[(df["Purchase Date"] <= one_year_ago) & (df["Purchase Date"] > two_years_ago) & (df["Debt Level"] == "HD")]
    two_years_hd_df = df[(df["Purchase Date"] <= two_years_ago) & (df["Purchase Date"] > three_years_ago) & (df["Debt Level"] == "HD")]
    two_years_ld_df = df[(df["Purchase Date"] <= two_years_ago) & (df["Purchase Date"] > three_years_ago) & (df["Debt Level"] == "LD")]
    three_years_df = df[df["Purchase Date"] <= three_years_ago]

    return one_year_df, two_years_hd_df, two_years_ld_df, three_years_df

# Function to download template
def download_template():
    sample_data = [
        ("AAPL", "2023-06-10", 200, "HD"),
        ("MHK", "2023-06-12", 135, "LD"),
        ("GOOGL", "2022-05-20", 2500, "HD"),
        ("AMZN", "2021-03-15", 3500, "LD"),
        ("TSLA", "2020-01-30", 900, "HD"),
        ("MSFT", "2022-07-25", 300, "LD")
    ]
    template_df = pd.DataFrame(sample_data, columns=["Ticker", "Purchase Date", "Target Price", "Debt Level"])
    template_df.to_csv("template.txt", index=False)
    with open("template.txt") as f:
        st.download_button("Download Template", f, file_name="template.txt")

# Function to load uploaded file
def load_uploaded_file(uploaded_file):
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file, parse_dates=["Purchase Date"])
        st.session_state.df = df

# Main function to render the Streamlit page
def main():
    if not check_password():
        st.stop()  # Do not continue if check_password is not True.
    
    st.markdown("## Price & Time Checker")
    st.write("Download the template, update the data, and upload the updated file.")

    # Download template
    download_template()

    # Upload updated file
    uploaded_file = st.file_uploader("Upload Updated File", type=["txt"])
    load_uploaded_file(uploaded_file)

    if st.session_state.df is None:
        st.warning("Please upload the data file to proceed.")
    else:
        # Check stocks that reached target prices
        with st.spinner(f"Updating {len(st.session_state.df)} ticker prices ..."):
            reached_target_df = check_target_prices(st.session_state.df)

        # Classify stocks by holding period
        one_year_df, two_years_hd_df, two_years_ld_df, three_years_df = classify_by_holding_period(st.session_state.df)

        # Display results using st.dataframe
        st.header("Stocks Reaching the Target Price")
        st.dataframe(reached_target_df, use_container_width=True, hide_index=True)

        st.header("High Debt Stocks Held for One Year")
        one_year_df["Reached One Year On"] = one_year_df["Purchase Date"] + timedelta(days=365)
        one_year_df = one_year_df.sort_values(by="Reached One Year On")
        st.dataframe(one_year_df[["Ticker", "Reached One Year On"]], use_container_width=True, hide_index=True)

        st.header("Stocks Held for Two Years with High Debt")
        two_years_hd_df["Reached Two Years On"] = two_years_hd_df["Purchase Date"] + timedelta(days=730)
        two_years_hd_df = two_years_hd_df.sort_values(by="Reached Two Years On")
        st.dataframe(two_years_hd_df[["Ticker", "Reached Two Years On"]], use_container_width=True, hide_index=True)

        st.header("Stocks Held for Two Years with Low Debt")
        two_years_ld_df["Reached Two Years On"] = two_years_ld_df["Purchase Date"] + timedelta(days=730)
        two_years_ld_df = two_years_ld_df.sort_values(by="Reached Two Years On")
        st.dataframe(two_years_ld_df[["Ticker", "Reached Two Years On"]], use_container_width=True, hide_index=True)

        st.header("Stocks Held for Three Years")
        three_years_df["Reached Three Years On"] = three_years_df["Purchase Date"] + timedelta(days=1095)
        three_years_df = three_years_df.sort_values(by="Reached Three Years On")
        st.dataframe(three_years_df[["Ticker", "Reached Three Years On"]], use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
