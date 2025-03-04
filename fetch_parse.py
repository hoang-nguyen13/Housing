import os
import re
import pandas as pd
import numpy as np

def process_real_estate_data():
    """
    This function processes real estate data from multiple CSV files, cleans and merges them into a single DataFrame.
    
    Args:
        None
    
    Returns:
        None
    """
    # List of district CSV files
    district_files = [
        'data/filtered_real_estate_listings_thanh-xuan.csv',
        'data/filtered_real_estate_listings_ba-dinh.csv',
        'data/filtered_real_estate_listings_cau-giay.csv',
        'data/filtered_real_estate_listings_nam-tu-liem.csv',
        'data/filtered_real_estate_listings_bac-tu-liem.csv',
        'data/filtered_real_estate_listings_hai-ba-trung.csv',
        'data/filtered_real_estate_listings_hoan-kiem.csv',
        'data/filtered_real_estate_listings_dong-da.csv',
        'data/filtered_real_estate_listings_ha-dong.csv',
        'data/filtered_real_estate_listings_hoang-mai.csv',
        'data/filtered_real_estate_listings_long-bien.csv',
        'data/filtered_real_estate_listings_tay-ho.csv'
    ]

    # Initialize an empty list to hold dataframes
    dfs = []

        # Loop through each district file
    for file in district_files:
        if os.path.exists(file):
            # Check if file is empty (0 bytes)
            if os.path.getsize(file) == 0:
                print(f"Skipping empty file (0 bytes): {file}")
                continue
            
            try:
                df = pd.read_csv(file)  # Read the CSV file into a DataFrame
                if df.empty:
                    print(f"Skipping file with no data (but has headers): {file}")
                    continue
                dfs.append(df)
                print(f"Loaded data from: {file}")
            except pd.errors.EmptyDataError:
                print(f"Skipping completely empty file (no headers): {file}")
                continue
            except Exception as e:
                print(f"Error reading {file}: {e}")
                continue
        else:
            print(f"File does not exist: {file}")

    # Check if there’s any data to process
    if not dfs:
        print("No valid data to process. Exiting.")
        return
    # Concatenate all dataframes into one
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Remove duplicates based on "Product ID" and "Date Posted", keeping the first occurrence
    # if "Id" in merged_df.columns and "Date Posted" in merged_df.columns:
    #     initial_rows = len(merged_df)
    #     merged_df = merged_df.drop_duplicates(subset=["Id", "Date Posted"], keep='first')
    #     duplicates_removed = initial_rows - len(merged_df)
    #     print(f"Removed {duplicates_removed} duplicate rows based on 'Product ID' and 'Date Posted'.")

    # Ensure "Location" column exists before sorting
    if "Location" in merged_df.columns:
        # Sort by "Location" in ascending alphabetical order
        merged_df = merged_df.sort_values(by="Location", ascending=True)

    # Count the number of empty cells (NaN) per row
    merged_df["empty_count"] = merged_df.isna().sum(axis=1)

    # Remove rows that have 3 or more empty cells
    merged_df = merged_df[merged_df["empty_count"] < 3]

    # Sort: 
    # 1️⃣ Rows with no empty cells first
    # 2️⃣ Then rows with 1 or more empty cells at the bottom
    merged_df = merged_df.sort_values(by=["empty_count", "Location"], ascending=[True, True]).drop(columns=["empty_count"])

    # Clean "Price per m²" column
    if "Price per m²" in merged_df.columns:
        # Separate rows with nghìn/m², tỉ/m², đồng/m² and keep them at the bottom
        nghin_ti_dong_rows = merged_df[merged_df["Price per m²"].astype(str).str.contains(r'nghìn/m²|tỉ/m²|đồng/m²', na=False)]
        valid_rows = merged_df[~merged_df["Price per m²"].astype(str).str.contains(r'nghìn/m²|tỉ/m²|đồng/m²', na=False)]
        
        # Clean the valid rows (those that are not nghìn/m², tỉ/m², đồng/m²)
        valid_rows.loc[:, "Price per m²"] = valid_rows["Price per m²"].astype(str).apply(lambda x: re.sub(r'\s*tr/m²', '', x))  # Remove tr/m²
        valid_rows.loc[:, "Price per m²"] = valid_rows["Price per m²"].apply(lambda x: x.replace(",", "."))  # Replace commas with periods
        valid_rows.loc[:, "Price per m²"] = pd.to_numeric(valid_rows["Price per m²"], errors='coerce')  # Convert to float
        
        # Concatenate the valid rows and nghìn/tỉ/dồng rows (with nghìn/tỉ/dồng at the bottom)
        merged_df = pd.concat([valid_rows, nghin_ti_dong_rows], ignore_index=True)
    # Remove rows where 'Price per m²' contain "nghìn", "tỉ", or "đồng"
    merged_df = merged_df[~merged_df["Price per m²"].str.contains("nghìn|tỉ|đồng", na=False)]

    if "Price" in merged_df.columns:
        # Define a function to clean and process the price values
        def clean_price(price):
            if isinstance(price, str):
                # Check if "Giá thỏa thuận", if yes, return the same value
                if "Giá thỏa thuận" in price:
                    return price
                
                # Remove "tỉ", replace "," with ".", and convert to float
                if "tỷ" in price:
                    price = re.sub(r"\s*tỷ", "", price)  # Remove "tỉ"
                    price = price.replace(",", ".")  # Replace comma with period
                    try:
                        return float(price)  # Convert to float and multiply by 1 million (tỉ to đồng)
                    except ValueError:
                        return None  # Handle cases where conversion fails
                
                # Remove "triệu", replace "," with ".", convert to float, and divide by 1000
                if "triệu" in price:
                    price = re.sub(r"\s*triệu", "", price)  # Remove "triệu"
                    price = price.replace(",", ".")  # Replace comma with period
                    try:
                        return float(price) / 1000  # Convert to float and multiply by 1000 (triệu to đồng)
                    except ValueError:
                        return None  # Handle cases where conversion fails
            return price  # Return the price as is if no conditions match

        # Apply the cleaning function to the "Price" column
        merged_df["Price"] = merged_df["Price"].apply(clean_price)
        
    if "Area" in merged_df.columns:
        # Define a function to clean and process the area values
        def clean_area(area):
            if isinstance(area, str):
                area = area.replace("m²", "")  # Remove "m²"
                area = area.replace(",", ".")  # Replace comma with period
                try:
                    return float(area)  # Convert to float
                except ValueError:
                    return None  # Handle cases where conversion fails
            return area  # Return the value as is if it's not a string

        # Apply the cleaning function to the "Area" column
        merged_df["Area"] = merged_df["Area"].apply(clean_area)

    # Remove all duplicate rows (not keeping any)
    merged_df = merged_df[~merged_df.duplicated(keep=False)]

    # Convert "Price" to string first to ensure compatibility for replacement
    merged_df["Price"] = merged_df["Price"].astype(str)

    # Replace "Giá thỏa thuận" with -1, then convert to float
    merged_df["Price"] = merged_df["Price"].replace("Giá thỏa thuận", -1, regex=False)
    merged_df["Price"] = pd.to_numeric(merged_df["Price"], errors='coerce')  # Convert to float, errors become NaN

    # Convert "Price per m²" to float
    merged_df["Price per m²"] = pd.to_numeric(merged_df["Price per m²"], errors='coerce')
    merged_df["Price per m²"] = merged_df["Price per m²"].replace(np.nan, 0)
    # Convert "Area" to float
    merged_df["Area"] = merged_df["Area"].replace(" m²", "")  # Remove "m²" if exists
    merged_df["Area"] = pd.to_numeric(merged_df["Area"], errors='coerce')

    merged_df.insert(merged_df.columns.get_loc("Price per m²") + 1, "calc price", None)

    # Ensure "Price per m²" and "Area" are numeric, then calculate "calc price"
    merged_df["Price per m²"] = pd.to_numeric(merged_df["Price per m²"], errors='coerce')
    merged_df["Area"] = pd.to_numeric(merged_df["Area"], errors='coerce')

    # Calculate the "calc price" and insert it into the column
    merged_df["calc price"] = (merged_df["Price per m²"] * merged_df["Area"]) / 1000
    merged_df["calc price"] = merged_df["calc price"].apply(lambda x: round(x, 2))
    merged_df["calc price"] = (merged_df["calc price"] - merged_df["Price"]).apply(lambda x: round(x, 3))

    # Remove rows where "calc price" deviates too much from 0, except for -1
    tolerance = 1.1  # Define a tolerance level
    merged_df = merged_df[(merged_df["calc price"].abs() <= tolerance) | (merged_df["calc price"] == -1)]

    # Save the sorted dataframe back to a CSV
    merged_df.to_csv('data/merged_real_estate_listings.csv', index=False)

    print("✔ Data has been merged, sorted alphabetically by 'Location'.")
    print("✔ Rows with 3 or more empty cells have been removed.")
    print("✔ 'Price per m²' column has been cleaned.")
    print("✔ Rows with invalid 'Price per m²' values have been removed.")
    print("✔ 'Price' column has been cleaned.")
    print("✔ 'Area' column has been cleaned.")
    print("✔ Duplicate rows have been removed.")
    print("✔ 'Giá thỏa thuận' values have been replaced with -1 in 'Price' column.")
    print("✔ 'calc price' column has been calculated and added.")
    print("✔ Rows with 'calc price' deviating too much from 0 have been removed.")
    print("✔ Final data has been saved to 'data/merged_real_estate_listings.csv'.")
    
    return None