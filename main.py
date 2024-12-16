import gzip
import logging
import time
import json

import pandas as pd
import requests
import os
import streamlit as st

from io import StringIO, BytesIO

# Set up logging with time and date
logging.basicConfig(
    filename='analyze_idq_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Marketplace API setup
MARKETPLACE_BASE_URL = st.secrets["MARKETPLACE_BASE_URL"]
AWS_CLIENT_ID = st.secrets["AWS_CLIENT_ID"]
AWS_CLIENT_SECRET = st.secrets["AWS_CLIENT_SECRET"]
AWS_TOKEN_URL = st.secrets["AWS_TOKEN_URL"]
AWS_REFRESH_TOKEN = st.secrets["AWS_REFRESH_TOKEN"]
marketplace_name = "amazon"
# Initialize session state for keeping track of file paths
if "output_file" not in st.session_state:
    st.session_state.output_file = None

def analyze_idq(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
        # Filter for products with review scores above 0.1 but below 3.5
        filtered_df = df[(df['Review Avg Rating'] > 0.1) & (df['Review Avg Rating'] < 3.5)]
        grouped = filtered_df.groupby('Marketplace')
        F1_output = BytesIO()
        #output_file = 'F1s.xlsx'
        with pd.ExcelWriter(F1_output, engine='xlsxwriter') as writer:
            for name, group in grouped:
                group[['ASIN']].to_excel(writer, sheet_name=name, index=False)
        F1_output.seek(0)
        # Save the file in Streamlit session state so it can be used later
        st.session_state.output_file = F1_output
        return True
    except Exception as e:
        #logging.error(f"An unexpected error occurred during the initial IDQ analysis: {e}")
        st.error(f"An unexpected error occurred during the initial IDQ analysis: {e}")

def update_excel_with_seller_sku(access_token):
    marketplace_id_mapping = {
        "UK": "A1F83G8C2ARO7P",
        "DE": "A1PA6795UKMFR9",
        "FR": "A13V1IB3VIYZZH",
        "NL": "A1805IZSGTT6HS",
        "BE": "AMEN7PMS3EDWL",
        "ES": "A1RKKUPIHCS9HS",
        "IT": "APJ6JRA9NG5V4",
        "PL": "A1C3SOZRARQ6R3",
        "SE": "A2NODRKZP88ZB9"
    }
    try:
        logging.info("Starting to update F1s.xlsx with Seller SKU.")

        # Load the Excel file from session state
        input_file = st.session_state.output_file

        # Read the Excel file into a DataFrame
        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names
        #st.info(f"Found sheet names: {sheet_names}")

        # Store dataframes temporarily
        df_dict = {}

        # Read and process each sheet, then store in df_dict
        for sheet in sheet_names:
            marketplace_id = marketplace_id_mapping.get(sheet)
            logging.info(f"Processing sheet: {sheet}")

            # Read the Excel sheet into a DataFrame
            df_excel = pd.read_excel(input_file, sheet_name=sheet)

            # Read the corresponding .txt file into a DataFrame (assume the file is uploaded as well)
            df_txt = get_product_listing(access_token,marketplace_id)
            #df_txt = read_txt_file(sheet)
            #st.write("run successfully")

            # Check if df_txt is None and handle the error
            if df_txt is None:
                logging.error(f"Skipping sheet {sheet} due to errors in reading the .txt file.")
                st.error(f"Skipping sheet {sheet} due to errors in reading the .txt file.")
                continue

            # Check if required columns are in df_txt
            if 'asin1' not in df_txt.columns or 'seller-sku' not in df_txt.columns:
                logging.error(
                    f"Required columns 'asin1' or 'seller-sku' are missing in the .txt file for sheet {sheet}.")
                continue

            # Merge the two DataFrames based on the 'ASIN' and 'asin1' columns using an outer join to identify missing ASINs
            merged_df = pd.merge(df_excel, df_txt[['asin1', 'seller-sku']], left_on='ASIN', right_on='asin1',
                                 how='inner', indicator=True)

            # Log and drop rows where ASIN is no longer listed in the .txt file
            missing_asins = merged_df[merged_df['_merge'] == 'left_only']
            for _, row in missing_asins.iterrows():
                logging.warning(f"ASIN {row['ASIN']} IS NO LONGER LISTED IN {sheet}")
                st.warning(f"ASIN {row['ASIN']} IS NO LONGER LISTED IN {sheet}")
            merged_df = merged_df[merged_df['_merge'] != 'left_only']

            # Drop the 'asin1' and '_merge' columns as they are redundant
            merged_df.drop(columns=['asin1', '_merge'], inplace=True)

            # Rename the 'seller-sku' column to 'Seller SKU'
            merged_df.rename(columns={'seller-sku': 'Seller SKU'}, inplace=True)

            df_dict[sheet] = merged_df
        del xls

        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)
                st.write("ok dokie")

        output.seek(0)

        # Store the updated file in session state
        st.session_state.output_file = output

        st.info("Successfully updated F1s.xlsx with Seller SKU information.")
        return True

    except Exception as e:
        #logging.error(f"An error occurred while updating the Excel file: {e}")
        st.error(f"An error occurred while updating the Excel file: {e}")


def update_excel_with_sku_description():
    try:
        logging.info("Starting to update F1s.xlsx with SKU description.")
        print("Starting to update F1s.xlsx with SKU description.")

        # Open the existing Excel file for reading
        input_file = 'F1s.xlsx'
        output_file = 'F1s - Desc Added.xlsx'
        csv_file = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS_mN7-KwnH2aN-afhBMbM_1IlBylxwgJByEkQU5M3HJQuSDx8-pk3HwaJ5TOLgNeD0SGcdgHikloFK/pub?gid=788370787&single=true&output=csv'

        # Read the CSV file into a DataFrame
        df_csv = pd.read_csv(csv_file, header=2)

        # Open the Excel file for reading sheet names
        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names
        logging.info(f"Found sheet names: {sheet_names}")

        # Store dataframes temporarily
        df_dict = {}

        # Read and process each sheet, then store in df_dict
        for sheet in sheet_names:
            logging.info(f"Processing sheet: {sheet}")

            # Read the Excel sheet into a DataFrame
            df_excel = pd.read_excel(input_file, sheet_name=sheet)

            # Log the column names for debugging
            logging.info(f"Columns in {sheet}: {df_excel.columns.tolist()}")

            # Check if 'Seller SKU' exists in df_excel
            if 'Seller SKU' in df_excel.columns:
                # Create a lookup column without the F1, F2, F3, etc. suffix
                df_excel['SKU Lookup'] = df_excel['Seller SKU'].str.replace(r'F\d+$', '', regex=True)

                # Merge the Excel DataFrame and the CSV DataFrame based on 'SKU Lookup' and 'Sku code'
                logging.info(f"Merging SKU description for sheet {sheet}.")
                merged_df = pd.merge(df_excel, df_csv[['Sku code', 'Sku description']], left_on='SKU Lookup',
                                     right_on='Sku code', how='left')

                # Drop the 'Sku code' and 'SKU Lookup' columns as they're redundant
                merged_df.drop(columns=['Sku code', 'SKU Lookup'], inplace=True)
            else:
                logging.warning(f"'Seller SKU' column not found in {sheet}. Skipping SKU description merge.")
                merged_df = df_excel

            df_dict[sheet] = merged_df

        # Close the read operation
        del xls

        # Open a new Excel writer and write data
        with pd.ExcelWriter(output_file) as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)

        logging.info("Successfully updated F1s.xlsx with SKU description information. Saved as F1s - Desc Added.xlsx.")

    except Exception as e:
        #logging.error(f"An error occurred while updating the Excel file with SKU description: {e}")
        st.error(f"An error occurred while updating the Excel file with SKU description: {e}")


def update_excel_with_f1_to_use():
    try:
        logging.info("Starting to update F1s - Desc Added.xlsx with F1 to Use.")
        print("Starting to update F1s - Desc Added.xlsx with F1 to Use.")

        # Open the existing Excel file for reading
        input_file = 'F1s - Desc Added.xlsx'
        output_file = 'F1s - Desc Added with F1 to Use.xlsx'

        # Fetch the CSV file from the URL
        url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRxBqpSTMwezeOji3KXDlrp3855sQHFuYxmKsCIDwILg4iHMEx2BBmp87nwEgI__4g3rM6H65rIp0sF/pub?gid=0&single=true&output=csv"
        response = requests.get(url)
        csv_data = StringIO(response.text)
        df_csv = pd.read_csv(csv_data)

        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names
        logging.info(f"Found sheet names: {sheet_names}")

        # Store dataframes temporarily
        df_dict = {}

        # Read and process each sheet, then store in df_dict
        for sheet in sheet_names:
            logging.info(f"Processing sheet: {sheet}")

            # Read the Excel sheet into a DataFrame
            df_excel = pd.read_excel(input_file, sheet_name=sheet)

            # Check if 'Seller SKU' exists in df_excel
            if 'Seller SKU' in df_excel.columns:
                # Initialize an empty list to hold F1 to Use values
                f1_to_use_values = []

                for sku in df_excel['Seller SKU']:
                    # Search for the SKU in columns B to P of the CSV DataFrame
                    found_row = df_csv.iloc[:, 1:16][
                        df_csv.iloc[:, 1:16].apply(lambda row: row.astype(str).str.contains(str(sku), na=False).any(),
                                                   axis=1)]  # Search for SKU in columns B to P

                    if not found_row.empty:
                        # Take the last non-empty value from the row
                        last_non_empty_value = found_row.iloc[0, :].dropna().iloc[-1]
                        f1_to_use_values.append(last_non_empty_value)
                    else:
                        f1_to_use_values.append(None)

                # Add the F1 to Use column to the DataFrame
                df_excel['F1 to Use'] = f1_to_use_values
                df_dict[sheet] = df_excel
            else:
                logging.warning(f"'Seller SKU' column not found in sheet {sheet}. Skipping this sheet.")

        # Close the read operation
        del xls

        # Open a new Excel writer and write data
        with pd.ExcelWriter(output_file) as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)

        logging.info(
            "Successfully updated F1s - Desc Added.xlsx with F1 to Use information. Saved as F1s - Desc Added with F1 to Use.xlsx.")
    except Exception as e:
        st.error(f"An error occurred while updating the Excel file with F1 to Use: {e}")


def update_excel_with_barcodes(uploaded_barcodes):
    try:
        logging.info("Starting to update F1s - Desc Added with F1 to Use.xlsx with Barcodes.")
        print("Starting to update F1s - Desc Added with F1 to Use.xlsx with Barcodes.")

        # Open the existing Excel file for reading (already present in the backend)
        input_file = 'F1s - Desc Added with F1 to Use.xlsx'
        output_file = 'F1s - Barcode.xlsx'

        # Read the uploaded barcodes.csv file into a DataFrame, headers are on the 4th row (index 3)
        df_barcodes = pd.read_csv(uploaded_barcodes, header=3)

        xls = pd.ExcelFile(input_file)
        sheet_names = xls.sheet_names
        logging.info(f"Found sheet names: {sheet_names}")

        # Store dataframes temporarily
        df_dict = {}

        # Read and process each sheet, then store in df_dict
        for sheet in sheet_names:
            logging.info(f"Processing sheet: {sheet}")

            # Read the Excel sheet into a DataFrame
            df_excel = pd.read_excel(input_file, sheet_name=sheet)

            # Check if 'F1 to Use' exists in df_excel
            if 'F1 to Use' in df_excel.columns:
                # Initialize empty lists to hold Barcode and GS1 Brand values
                barcode_values = []
                gs1_brand_values = []

                for f1 in df_excel['F1 to Use']:
                    found_row = df_barcodes[df_barcodes['SKU'] == f1]

                    if not found_row.empty:
                        # Take the value from the "Number" column, remove characters like = or ", and add to barcode_values
                        number_value = str(found_row['Number'].iloc[0]).replace('=', '').replace('"', '')
                        barcode_values.append(number_value)

                        # Take the value from the "Main Brand" column and add to gs1_brand_values
                        gs1_brand_value = found_row['Main Brand'].iloc[0]
                        gs1_brand_values.append(gs1_brand_value)
                    else:
                        barcode_values.append(None)
                        gs1_brand_values.append(None)

                # Add the Barcode and GS1 Brand columns to the DataFrame
                df_excel['EAN'] = barcode_values
                df_excel['GS1 Brand'] = gs1_brand_values

                df_dict[sheet] = df_excel
            else:
                logging.warning(f"'F1 to Use' column not found in sheet {sheet}. Skipping this sheet.")

        # Close the read operation
        del xls

        # Open a new Excel writer and write data
        with pd.ExcelWriter(output_file) as writer:
            for sheet, df in df_dict.items():
                logging.info(f"Writing updated data to sheet {sheet}.")
                df.to_excel(writer, sheet_name=sheet, index=False)

        logging.info(
            "Successfully updated F1s - Desc Added with F1 to Use.xlsx with Barcodes. Saved as F1s - Barcode.xlsx."
        )

        # Store the output file path in session state so it can be downloaded later
        st.session_state.output_file = output_file

    except Exception as e:
        st.error(f"An error occurred while updating the Excel file with Barcodes: {e}")

def unzip_gzip_to_csv(gzip_data):
    # Unzip GZIP data and convert it to a CSV format
    try:
        with gzip.GzipFile(fileobj=BytesIO(gzip_data), mode='rb') as f:
            # Read the decompressed content into a DataFrame (assuming the data is CSV format)
            df = pd.read_csv(f, encoding='windows-1252', delimiter='\t')
            # Parse the DataFrame to keep only the 'seller-sku' and 'asin1' columns
    except (OSError, gzip.BadGzipFile) as e:
        print("Not a GZIP file. Trying as plain CSV...")
        # If decompression fails, treat it as a plain CSV file
        df = pd.read_csv(BytesIO(gzip_data), encoding='windows-1252', delimiter='\t')
    if 'seller-sku' in df.columns and 'asin1' in df.columns:
        parsed_df = df[['seller-sku', 'asin1']]
    elif 'seller-sku' in df.columns and 'product-id' in df.columns:
        df.rename(columns={'product-id': 'asin1'}, inplace=True)
        parsed_df = df[['seller-sku', 'asin1']]
    else:
        # If columns are not found, raise an error or handle accordingly
        parsed_df = None
        print("Error: 'seller-sku' and 'asin1' columns not found in the data.")
    return parsed_df

def get_access_token():
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': AWS_REFRESH_TOKEN,
        'client_id': AWS_CLIENT_ID,
        'client_secret': AWS_CLIENT_SECRET
    }
    try:
        marketplace_headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        # Make the request to get access token
        response = requests.post(AWS_TOKEN_URL, headers= marketplace_headers, data=payload)

        # Check if request was successful
        if response.status_code == 200:
            token_data = response.json()
            #print(f"response {token_data}.")
            access_token = token_data['access_token']
            return access_token
        else:
            st.info(f"Error fetching access token for amazon: {response.status_code}")
            return None
    except Exception as e:
        message = f"Exception occurred while fetching access token: {str(e)}"
        return None

def get_product_listing(access_token, marketplace_id):
    max_retries = 10  # Maximum number of retries
    retries = 0
    # API URL for creating a report
    api_url = f"{MARKETPLACE_BASE_URL}/reports/2021-06-30/reports"
    # API request headers
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json',
        'x-amz-access-token': access_token,
    }

    # body of the request
    body = {
        "reportType": "GET_MERCHANT_LISTINGS_DATA",
        "marketplaceIds": [marketplace_id]
    }
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(body))
        # Check if the request was successful
        if response.status_code == 202:  # Status 202 indicates the report request was accepted
            report_data = response.json()
            report_id = report_data.get('reportId')
            st.write(f"print  {report_id}")
            api_url = f"{MARKETPLACE_BASE_URL}/reports/2021-06-30/reports/{report_id}"
            while retries < max_retries:
                response_reports = requests.get(api_url, headers=headers)
                if response_reports.status_code == 200:
                    report_status = response_reports.json()
                    status = report_status.get("processingStatus")
                    if status in ("IN_QUEUE", "INPROGRESS", "IN_PROGRESS"):
                        time.sleep(30)
                        retries += 1
                    elif status == "DONE":
                        #st.write(f" Report Status: {status}")
                        report_document_id = report_status.get('reportDocumentId')
                        api_url = f"{MARKETPLACE_BASE_URL}/reports/2021-06-30/documents/{report_document_id}"
                        response = requests.get(api_url, headers=headers)
                        report_data = response.json()
                        download_url = report_data.get('url')
                        download_response = requests.get(download_url)
                        df_txt = unzip_gzip_to_csv(download_response.content)
                        #st.write(f" Report Status: {df_txt}")
                        return df_txt
            print("The process is taking longer than expected by amazon to generate the report. Try later")
            return None

    except Exception as e:
        message = f"Exception while submitting feed: {e}"
        return None


def main():
    st.set_page_config(page_title="IDQ File Processor", page_icon="📄")

    st.markdown(
        """
        <h1 style='text-align: center;'>
            🔄 Amazon F1s
        </h1>
        """,
        unsafe_allow_html=True
    )

    st.markdown("""<style>
        .css-1offfwp {padding-top: 1rem;}
        .css-1v3fvcr {background-color: #f8f9fa !important;}
        .block-container {padding: 7rem !important;}
        .stButton button {background-color: #4CAF50; color: white; border: none; border-radius: 5px; padding: 10px 20px; font-size: 16px; cursor: pointer;}
        .stButton button:hover {background-color: #45a049;}
        .stFileUploader {border: 2px dashed #4CAF50 !important; border-radius: 10px;}
        </style>""", unsafe_allow_html=True)
    # File uploader widget for the user to upload their IDQ file
    uploaded_file = st.file_uploader("Upload IDQ Excel file", type="xlsx")
    # File uploader widget for the user to upload their barcodes file
    uploaded_barcodes = st.file_uploader("Upload Barcode CSV file", type="csv")

    if uploaded_file is not None and uploaded_barcodes is not None and st.session_state.output_file is None:
        # When a file is uploaded, run the analysis
        with st.spinner("Processing your files. This may take a few moments..."):
            if analyze_idq(uploaded_file):
                access_token = get_access_token()
                if access_token:
                    if update_excel_with_seller_sku(access_token):
                        update_excel_with_sku_description()
                        update_excel_with_f1_to_use()
                        update_excel_with_barcodes(uploaded_barcodes)
    # Check if the output file exists and show download button
    # if st.session_state.output_file is not None:
    #     with open(st.session_state.output_file, "rb") as file:
    #         st.download_button(label="Save File", data=file, file_name=st.session_state.output_file,
    #                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    main()