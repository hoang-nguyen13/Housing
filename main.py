from multiprocessing import Pool
from scraping import districts, scrape_district
from fetch_parse import process_real_estate_data
import os
import pandas as pd
def main():
    """
    Main function to scrape real estate listings for all districts in Hanoi
    and process the data.
    """
    # Ask user if they want to start scraping
    user_input = input("Do you want to start scraping? (yes/no): ").strip().lower()
    if user_input == 'yes':
        if len(districts) == 1:
            district = districts[0]
            csv_file_path = f"data/filtered_real_estate_listings_{district}.csv"
            os.makedirs("data", exist_ok=True)

            # Define page ranges for 3 processes (adjust max_pages as needed)
            max_pages = 10  # Estimate or dynamically determine total pages if possible
            pages_per_process = max_pages // 3
            page_ranges = [
                (1, pages_per_process),                         # Process 1: Pages 1-3
                (pages_per_process + 1, 2 * pages_per_process), # Process 2: Pages 4-6
                (2 * pages_per_process + 1, max_pages)          # Process 3: Pages 7-10
            ]

            with Pool(processes=3) as pool:
                # Run 3 processes, each scraping a range of pages
                results = pool.starmap(scrape_district, [(district, start, end) for start, end in page_ranges])
            
            # Combine all data into one list
            all_data = []
            for data in results:
                all_data.extend(data)

            if all_data:
                # Write combined data to a single CSV
                df = pd.DataFrame(all_data, columns=[
                    "Product ID", "Date Posted", "Product Title", "Price", "Area", "Price per m²", 
                    "Bedrooms", "Toilets", "Location", "Coordinates"
                ])
                df.to_csv(csv_file_path, index=False)
                print(f"All data saved to {csv_file_path}")
            else:
                print(f"No data scraped for {district}.")
        else:
            # Original logic for multiple districts
            with Pool(processes=3) as pool:
                pool.map(scrape_district, districts)
        print("Scraping completed for all districts.")
    else:
        print("Scraping skipped.")
    # Ask user if they want to start parsing
    user_input = input("Do you want to start parsing? (yes/no): ").strip().lower()
    if user_input == 'yes':
        print("Starting parsing...")
        process_real_estate_data()
        print("Parsing completed for all districts.")
    else:
        print("Parsing skipped.")
    

if __name__ == "__main__":
    main()