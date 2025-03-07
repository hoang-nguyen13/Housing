import os
import re
import pandas as pd
import numpy as np
import csv
def process_real_estate_data():
    """
    Processes real estate data from CSV files, splitting rows at 10 columns.
    Valid rows (first 10 fields) are kept, extra fields are saved to anomaly.csv.
    """
    district_files = []
    month_prefixes = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    def populate_district_files(data_dir='data'):
        for folder_name in os.listdir(data_dir):
            if any(folder_name.lower().startswith(month) for month in month_prefixes):
                folder_path = os.path.join(data_dir, folder_name)
                if os.path.isdir(folder_path):
                    for file_name in os.listdir(folder_path):
                        if (os.path.isfile(os.path.join(folder_path, file_name)) and 
                            file_name.startswith('filtered_real_estate_listings') and 
                            file_name.endswith('.csv')):
                            full_path = os.path.join(folder_path, file_name)
                            district_files.append(full_path)

    data_directory = 'data'
    populate_district_files(data_directory)

    dfs = []
    anomaly_data = []
    anomaly_dir = 'data/anomaly'
    os.makedirs(anomaly_dir, exist_ok=True)

    # Expected header for valid data
    expected_header = ["Id", "Date Posted", "Product Title", "Price", "Area", 
                       "Price per m²", "Bedrooms", "Toilets", "Location", "Coordinates"]

    for file in district_files:
        if not os.path.exists(file):
            print(f"File does not exist: {file}")
            continue
        if os.path.getsize(file) == 0:
            # print(f"Skipping empty file (0 bytes): {file}")
            continue

        try:
            # Read the file manually to split rows correctly
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, quotechar='"')
                header = next(reader)  # Get header
                if len(header) != 10:
                    # print(f"Skipping {file}: Header has {len(header)} fields, expected 10")
                    continue

                valid_data = []
                file_anomalies = []

                for i, row in enumerate(reader, 1):
                    if len(row) > 10:
                        # Split into valid (first 10) and anomaly (rest)
                        valid_row = row[:10]
                        anomaly_row = row[10:]
                        valid_data.append(valid_row)
                        file_anomalies.append(row)  # Save full row for anomaly
                        # print(f"Line {i} in {file}: Split {len(row)} fields - kept 10, marked {len(anomaly_row)} as anomaly")
                    elif len(row) == 10:
                        valid_data.append(row)
                    else:
                        # print(f"Line {i} in {file}: Found {len(row)} fields, expected 10 - added to anomalies")
                        file_anomalies.append(row)

                # Process valid data
                if valid_data:
                    df = pd.DataFrame(valid_data, columns=expected_header)
                    dfs.append(df)
                    # print(f"Loaded {len(valid_data)} valid rows from {file}")

                # Handle anomalies
                if file_anomalies:
                    anomaly_data.extend([header] + file_anomalies)  # Include header for context
                    # print(f"Found {len(file_anomalies)} anomalous rows in {file}, added to anomaly data")

        except Exception as e:
            print(f"Error processing {file}: {e}")
            continue

    # Save anomalies to a separate CSV with header only once
    if anomaly_data:
        anomaly_file = os.path.join(anomaly_dir, 'anomaly.csv')
        with open(anomaly_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(expected_header)  # Write header once
            writer.writerows(anomaly_data)    # Write all anomaly rows
        print(f"Saved {len(anomaly_data)} anomalous rows to {anomaly_file}")

    if not dfs:
        print("No valid data to process. Exiting.")
        return

    merged_df = pd.concat(dfs, ignore_index=True)
    
    os.makedirs('data/relevance', exist_ok=True)

    merged_df.to_csv('data/relevance/merged_real_estate_listings.csv', index=False)

    if "Location" in merged_df.columns:
        merged_df = merged_df.sort_values(by="Location", ascending=True)

    merged_df["empty_count"] = merged_df.isna().sum(axis=1)
    merged_df = merged_df[merged_df["empty_count"] < 3]
    merged_df = merged_df.sort_values(by=["empty_count", "Location"], ascending=[True, True]).drop(columns=["empty_count"])

    if "Price per m²" in merged_df.columns:
        nghin_ti_dong_rows = merged_df[merged_df["Price per m²"].astype(str).str.contains(r'nghìn/m²|tỉ/m²|đồng/m²', na=False)]
        valid_rows = merged_df[~merged_df["Price per m²"].astype(str).str.contains(r'nghìn/m²|tỉ/m²|đồng/m²', na=False)]
        valid_rows.loc[:, "Price per m²"] = valid_rows["Price per m²"].astype(str).apply(lambda x: re.sub(r'\s*tr/m²', '', x))
        valid_rows.loc[:, "Price per m²"] = valid_rows["Price per m²"].apply(lambda x: x.replace(",", "."))
        valid_rows.loc[:, "Price per m²"] = pd.to_numeric(valid_rows["Price per m²"], errors='coerce')
        merged_df = pd.concat([valid_rows, nghin_ti_dong_rows], ignore_index=True)
        merged_df = merged_df[~merged_df["Price per m²"].astype(str).str.contains("nghìn|tỉ|đồng", na=False)]

    if "Price" in merged_df.columns:
        def clean_price(price):
            if isinstance(price, str):
                if "Giá thỏa thuận" in price:
                    return price
                if "tỷ" in price:
                    price = re.sub(r"\s*tỷ", "", price)
                    price = price.replace(",", ".")
                    try:
                        return float(price)
                    except ValueError:
                        return None
                if "triệu" in price:
                    price = re.sub(r"\s*triệu", "", price)
                    price = price.replace(",", ".")
                    try:
                        return float(price) / 1000
                    except ValueError:
                        return None
            return price

        merged_df["Price"] = merged_df["Price"].apply(clean_price)

    if "Area" in merged_df.columns:
        def clean_area(area):
            if isinstance(area, str):
                area = area.replace("m²", "").replace(",", ".")
                try:
                    return float(area)
                except ValueError:
                    return None
            return area

        merged_df["Area"] = merged_df["Area"].apply(clean_area)

    merged_df = merged_df[~merged_df.duplicated(keep=False)]
    merged_df["Price"] = merged_df["Price"].astype(str).replace("Giá thỏa thuận", -1, regex=False)
    merged_df["Price"] = pd.to_numeric(merged_df["Price"], errors='coerce')
    merged_df["Price per m²"] = pd.to_numeric(merged_df["Price per m²"], errors='coerce').replace(np.nan, 0)
    merged_df["Area"] = pd.to_numeric(merged_df["Area"], errors='coerce')

    merged_df.insert(merged_df.columns.get_loc("Price per m²") + 1, "calc price", None)
    merged_df["calc price"] = (merged_df["Price per m²"] * merged_df["Area"]) / 1000
    merged_df["calc price"] = merged_df["calc price"].apply(lambda x: round(x, 2))
    merged_df["calc price"] = (merged_df["calc price"] - merged_df["Price"]).apply(lambda x: round(x, 3))

    tolerance = 1.1
    merged_df = merged_df[(merged_df["calc price"].abs() <= tolerance) | (merged_df["calc price"] == -1)]

    if "Id" in merged_df.columns and "Date Posted" in merged_df.columns:
        initial_rows = len(merged_df)  # Number of rows before deduplication
        print(f"Number of rows before deduplication: {initial_rows}")
        merged_df = merged_df.drop_duplicates(subset=["Id", "Date Posted"], keep='first')
        final_rows = len(merged_df)  # Number of rows after deduplication
        print(f"Number of rows after deduplication: {final_rows}")

      # Sort by Date Posted, then Location, then Price
    merged_df = merged_df.sort_values(by=["Date Posted", "Location", "Price"], ascending=[False, True, True])
    merged_df.to_csv('data/relevance/merged_real_estate_listings_parsed.csv', index=False)