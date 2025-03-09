from multiprocessing import Pool, Value
from scraping import scrape_district
from fetch_parse import process_real_estate_data
from ML import house_prediction
from tqdm import tqdm
import sys
import subprocess
import os

def init_counter(counter):
    """Initialize the shared counter for multiprocessing."""
    global district_counter
    district_counter = counter

def main():
    """
    Main function to scrape real estate listings, process data, and build model based on command-line arguments.
    """
    os.environ['PYTHONUNBUFFERED'] = '1'
    counter = Value('i', 0)

    try:
        # Parse command-line arguments
        args = sys.argv[1:]
        districts_to_scrape = [arg for arg in args if arg not in ['--parse', '--no-parse', '--build-model', '--no-build-model']]
        do_parse = '--parse' in args
        do_build_model = '--build-model' in args

        # Scrape if districts are provided
        if districts_to_scrape:
            total_districts_to_scrape = len(districts_to_scrape)
            print(f"Starting scraping for {total_districts_to_scrape} district(s): {', '.join(districts_to_scrape)}...")
            with Pool(processes=12, initializer=init_counter, initargs=(counter,)) as pool:
                try:
                    results = pool.imap_unordered(scrape_district, districts_to_scrape)
                    for _ in tqdm(results, total=total_districts_to_scrape, desc="Scraping Districts", unit="district", file=sys.stdout):
                        pass
                    print(f"\nNumber of districts scraped: {total_districts_to_scrape}/{total_districts_to_scrape}")
                    print("Scraping completed for selected districts.")
                except KeyboardInterrupt:
                    print("\nScraping interrupted! Cleaning up...")
                    pool.terminate()
                    pool.join()
                    print("Scraping process terminated safely.")
                    return
                except Exception as e:
                    print(f"\nAn error occurred during scraping: {e}")
                    pool.close()
                    pool.join()
                    return
        else:
            print("No districts provided; skipping scraping.")

        # Parsing
        if do_parse:
            print("Starting parsing...")
            process_real_estate_data()
            print("Parsing completed for all districts.")
        else:
            print("Parsing skipped.")

        # Prediction model
        if do_build_model:
            print("Building prediction model...")
            house_prediction()
            print("Prediction model done!")
            print("Starting app.py...")
            try:
                subprocess.run(["streamlit", "run", "app.py"], check=True)
                print("app.py executed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error running app.py: {e}")
            except FileNotFoundError:
                print("app.py not found. Please ensure it exists in the same directory.")
        else:
            print("Building prediction model skipped.")
            print("Starting app.py...")
            try:
                subprocess.run(["streamlit", "run", "app.py"], check=True)
                # print("app.py executed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error running app.py: {e}")
            except FileNotFoundError:
                print("app.py not found. Please ensure it exists in the same directory.")

    except KeyboardInterrupt:
        print("\nProcess interrupted! Exiting safely.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    main()