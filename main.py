import base64
import csv
import logging
import time
from io import BytesIO, TextIOWrapper

import pandas as pd
import requests
import streamlit as st

# Set up basic logging configuration
logging.basicConfig(
    level=logging.DEBUG,  # Set logging level to DEBUG, INFO, WARNING, ERROR
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Logs to the terminal (good for Streamlit Cloud)
        #logging.FileHandler("app.log")  # Logs to a file (optional)
    ]
)

# Marketplace API setup
MARKETPLACE_BASE_URL= st.secrets["MARKETPLACE_BASE_URL"]
BOL_CLIENT_ID = st.secrets["BOL_CLIENT_ID"]
BOL_CLIENT_SECRET= st.secrets["BOL_CLIENT_SECRET"]
BOL_TOKEN_URL= st.secrets["BOL_TOKEN_URL"]
marketplace_name = "bol"

# Initialize session state for keeping track of file paths
if "output_file" not in st.session_state:
    st.session_state.output_file = None

def analyze_listing(uploaded_file):
    try:
        df = pd.read_csv(uploaded_file)
        logging.info(f"Successfully read CSV file: {uploaded_file}, {len(df)} rows found.")
        return df
    except Exception as e:
        st.error(f"Error reading CSV file {uploaded_file}: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during the Processing of Listing File: {e}")
        st.error("An unexpected error occurred during the Processing of Listing File")

def update_excel_with_rating(listing_df, access_token):
    filtered_data = []
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/vnd.retailer.v9+json'
    }
    logging.info("Starting to update listing file with the ratings.")
    for index, row in listing_df.iterrows():
        ean = row['EAN']  # Make sure 'EAN' matches the exact column name in your local CSV
        ratings_response = get_product_ratings(ean, headers)
        ratings = ratings_response.get("ratings", []) if ratings_response else []

        # Filter ratings of 1, 2, or 3 with count > 0
        valid_ratings = [r['rating'] for r in ratings if r['rating'] in [1, 2, 3] and r['count'] > 0]

        if valid_ratings:
            min_rating = min(valid_ratings)
            filtered_data.append([ean, row['sku'], row['id'], min_rating])

        logging.info(f"Processed EAN: {ean} | SKU: {row['sku']}")
        # Delay for rate limiting
        time.sleep(1.2)
    return filtered_data

def write_filtered_ratings(data):
    logging.info(f"Writing filtered ratings to filtered_ratings.csv ...")
    try:
        # Create a BytesIO object to store the CSV data
        output = BytesIO()
        with TextIOWrapper(output, encoding='utf-8', newline='') as text_wrapper:
            writer = csv.writer(text_wrapper)
            writer.writerow(["ean", "sku", "id", "rating"])
            for row in data:
                writer.writerow(row)
        logging.info("Filtered ratings written to CSV successfully.")

        output.seek(0)
        # Store the updated file in session state
        st.session_state.output_file = output
        logging.info("Filtered ratings written to CSV successfully.")
    except Exception as e:
        st.error(f"Error writing filtered ratings to CSV: {e}")


def update_excel_with_sku_description():
    try:
        logging.info("Starting to update filtered_ratings.csv with SKU description.")
        print("Starting to update filtered_ratings.csv with SKU description.")

        # Load the Excel file from session state
        input_file = st.session_state.output_file
        csv_file = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS_mN7-KwnH2aN-afhBMbM_1IlBylxwgJByEkQU5M3HJQuSDx8-pk3HwaJ5TOLgNeD0SGcdgHikloFK/pub?gid=788370787&single=true&output=csv'

        # Read the CSV file into a DataFrame
        df_csv = pd.read_csv(csv_file, header=2)
        df_csv['Sku code'] = df_csv['Sku code'].astype(str)

        # Read the original filtered_ratings.csv into a DataFrame
        df_excel = pd.read_csv(input_file)
        df_excel['sku'] = df_excel['sku'].astype(str)

        # Merge based on 'sku' and 'Sku code'
        merged_df = pd.merge(df_excel, df_csv[['Sku code', 'Sku description']], left_on='sku',
                             right_on='Sku code', how='left')

        # Drop the 'Sku code' column as it's redundant
        merged_df.drop(columns=['Sku code'], inplace=True)

        # Save the merged DataFrame as an Excel file
        # Create a BytesIO object to store the CSV data
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            merged_df.to_excel(writer, sheet_name="Sheet1",index=False)
        logging.info("Filtered ratings written to CSV successfully.")

        output.seek(0)
        # Store the updated file in session state
        st.session_state.output_file = output
        logging.info("Successfully updated filtered_ratings file with SKU description information. Saved as filtered_ratings - Desc Added.xlsx")

    except Exception as e:
        logging.error(f"An error occurred while updating the Excel file with SKU description: {e}")
        st.error("An error occurred while updating the Excel file with SKU description")


def update_excel_with_f1_to_use():
    try:
        logging.info("Starting to update F1s - Desc Added.xlsx with F1 to Use.")
        print("Starting to update F1s - Desc Added.xlsx with F1 to Use.")

        # Load the existing Excel file from session state
        input_file = st.session_state.output_file

        # Fetch the CSV file from the URL
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRxBqpSTMwezeOji3KXDlrp3855sQHFuYxmKsCIDwILg4iHMEx2BBmp87nwEgI__4g3rM6H65rIp0sF/pub?gid=0&single=true&output=csv"
        df_csv = pd.read_csv(url)
        # Store dataframes temporarily
        df_dict = {}

        df_excel = pd.read_excel(input_file)
        df_excel['sku'] = df_excel['sku'].astype(str)

        f1_to_use_values = []
        for sku in df_excel['sku']:
            found_row = df_csv.iloc[:, 1:16].apply(
                lambda row: row.astype(str).str.contains(str(sku), na=False).any(), axis=1)

            matching_rows = df_csv[found_row]
            if not matching_rows.empty:
                last_non_empty_value = matching_rows.iloc[0, 1:16].dropna().iloc[-1]
                f1_to_use_values.append(last_non_empty_value)
            else:
                f1_to_use_values.append(None)

        df_excel['F1 to Use'] = f1_to_use_values
        # Write the updated data back to a BytesIO object
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)

        logging.info(f"Successfully updated Excel file with F1 to Use information.")
        output.seek(0)  # Reset the pointer of the BytesIO object
        st.session_state.output_file = output
    except Exception as e:
        st.error(f"An error occurred while updating the Excel file with F1 to Use: {e}")


def update_excel_with_barcodes(uploaded_barcodes):
    try:
        logging.info("Updating filtered_ratings_with_desc_and_F1_to_use.xlsx with Barcodes.")
        print("Updating filtered_ratings_with_desc_and_F1_to_use.xlsx with Barcodes.")

        input_file = st.session_state.output_file
        df_barcodes = pd.read_csv(uploaded_barcodes, header=3)

        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names

        df_dict = {}
        for sheet in sheet_names:
            logging.info(f"Processing sheet: {sheet}")
            df_excel = pd.read_excel(input_file, sheet_name=sheet)

            if 'F1 to Use' in df_excel.columns:
                barcode_values = []
                gs1_brand_values = []

                for f1 in df_excel['F1 to Use']:
                    found_row = df_barcodes[df_barcodes['SKU'] == f1]

                    if not found_row.empty:
                        number_value = str(found_row['Number'].iloc[0]).replace('=', '').replace('"', '')
                        barcode_values.append(number_value)

                        gs1_brand_value = found_row['Main Brand'].iloc[0]
                        gs1_brand_values.append(gs1_brand_value)
                    else:
                        barcode_values.append(None)
                        gs1_brand_values.append(None)

                df_excel['Barcode'] = barcode_values
                df_excel['GS1 Brand'] = gs1_brand_values
                df_dict[sheet] = df_excel
            else:
                logging.warning(f"'F1 to Use' column not found in sheet {sheet}. Skipping this sheet.")

        del xls

        # Write the updated data back to a BytesIO object
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)

        logging.info(f"Successfully updated {output} with Barcodes.")
        # Store the output file path in session state so it can be downloaded later
        output.seek(0)  # Reset the pointer of the BytesIO object
        st.session_state.output_file = output

    except Exception as e:
        logging.error(f"An error occurred while updating the Excel file with Barcodes: {e}")
        st.error("An error occurred while updating the Excel file with Barcodes")

def get_access_token():
    logging.info("Fetching access token...")
    credentials = f"{BOL_CLIENT_ID}:{BOL_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Accept": "application/json"
    }
    try:
        # Make the request to get access token
        response = requests.post(BOL_TOKEN_URL, headers=headers)

        # Check if request was successful
        if response.status_code == 200:
            token_data = response.json()
            #print(f"response {token_data}.")
            access_token = token_data['access_token']
            return access_token
        else:
            message = f"Error fetching access token: {response.status_code} - {response.text}"
            return None
    except Exception as e:
        message = f"Exception occurred while fetching access token: {str(e)}"
        return None

def get_product_ratings(ean, headers):
    logging.info(f"Fetching product ratings for EAN: {ean}")
    try:
        url = f"https://api.bol.com/retailer/products/{ean}/ratings"
        response = requests.get(url, headers=headers)

        # If response is 401 Unauthorized, reauthorize and retry
        if response.status_code == 401:
            logging.warning(f"401 Unauthorized error for EAN {ean}. Reauthorizing...")
            headers['Authorization'] = "Bearer " + get_access_token()
            response = requests.get(url, headers=headers)

        # If response is 404 Not Found, log and return None
        if response.status_code == 404:
            logging.warning(f"404 Not Found error for EAN {ean}. Skipping this EAN.")
            return None

        response.raise_for_status()
        logging.info(f"Successfully fetched ratings for EAN: {ean}")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred for EAN {ean}: {http_err}")
        return None
    except Exception as e:
        logging.error(f"An error occurred while getting product ratings for EAN {ean}: {e}")
        return None


def main():
    st.set_page_config(page_title="BOL File Processor", page_icon="📄")

    st.markdown(
        """
        <h1 style='text-align: center;'>
            🔄 BOL F1s
        </h1>
        """,
        unsafe_allow_html=True
    )

    st.markdown("""<style>
        .css-1offfwp {padding-top: 1rem;}
        .css-1v3fvcr {background-color: #f8f9fa !important;}
        .block-container {padding: 10rem !important;}
        .stButton button, .stDownloadButton button {background-color: #4CAF50; color: white; border: none; border-radius: 5px; padding: 10px 20px; font-size: 16px; cursor: pointer;}
        .stButton button:hover, .stDownloadButton button:hover {background-color: #45a049;}
        .stFileUploader {border: 2px dashed #4CAF50 !important; border-radius: 10px;}
        </style>""", unsafe_allow_html=True)
    # File uploader widget for the user to upload their IDQ file
    uploaded_file = st.file_uploader("Upload Listing CSV file", type="csv")
    # File uploader widget for the user to upload their barcodes file
    uploaded_barcodes = st.file_uploader("Upload Barcode CSV file", type="csv")

    if uploaded_file is not None and uploaded_barcodes is not None and st.session_state.output_file is None:
        # When a file is uploaded, run the analysis
        with st.spinner("Processing your files. This may take a few moments..."):
            listing_df = analyze_listing(uploaded_file)
            if listing_df is not None:
                access_token = get_access_token()
                if access_token:
                    filtered_rating_data = update_excel_with_rating(listing_df,access_token)
                    if filtered_rating_data:
                        write_filtered_ratings(filtered_rating_data)
                        update_excel_with_sku_description()
                        update_excel_with_f1_to_use()
                        update_excel_with_barcodes(uploaded_barcodes)
    # Check if the output file exists and show download button
    if st.session_state.output_file is not None:
        # Use Streamlit columns to place buttons side-by-side
        col1, col2, col3 = st.columns([0.1, 1, 1])
        # Column 1: Download Button
        with col2:
            st.download_button(label="Save File", data=st.session_state.output_file, file_name="F1_Barcodes.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Column 2: Trigger Asana Functionality
        with col3:
            if st.button("Create Asana Tasks"):
                st.info("Starting Asana task creation...")
                #create_asana_tasks_from_excel(send_to_asana=False)  # Call your function here
                st.success("Asana tasks created successfully!")

if __name__ == "__main__":
    main()