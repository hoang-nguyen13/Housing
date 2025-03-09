from multiprocessing import Pool, Value  # Import Pool for parallel tasks and Value for shared counter
from scraping import scrape_district  # Import districts list and scraping function
from fetch_parse import process_real_estate_data  # Import data processing function
from ML import house_prediction
from tqdm import tqdm  # Import tqdm for progress bars
import sys  # Import sys for output control
import subprocess  # Import subprocess to run app.py

def init_counter(counter):
    """Initialize the shared counter for multiprocessing."""
    global district_counter
    district_counter = counter

def scrape_with_counter(district):
    """Wrapper function to scrape a district and increment the counter."""
    scrape_district(district)
    with district_counter.get_lock():
        district_counter.value += 1

def main():
    """
    Main function to scrape real estate listings for districts provided via arguments,
    process the data, and activate app.py after model building.
    """
    counter = Value('i', 0)

    try:
        # Scrape districts provided via command-line arguments (from selector.js)
        if len(sys.argv) > 1:
            districts_to_scrape = sys.argv[1:]  # All arguments after script name
            total_districts_to_scrape = len(districts_to_scrape)
            print(f"Starting scraping for {total_districts_to_scrape} district(s): {', '.join(districts_to_scrape)}...")
            with Pool(processes=12, initializer=init_counter, initargs=(counter,)) as pool:
                try:
                    results = pool.imap_unordered(scrape_with_counter, districts_to_scrape)
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

        # Proceed with parsing and model building
        user_input = input("Do you want to start parsing? (yes/no): ").strip().lower()
        if user_input == 'yes':
            print("Starting parsing...")
            process_real_estate_data()
            print("Parsing completed for all districts.")
        else:
            print("Parsing skipped.")
        
        user_input = input("Do you want to build prediction model? (yes/no): ").strip().lower()
        if user_input == 'yes':
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
            print("Starting app.py...")
            try:
                subprocess.run(["streamlit", "run", "app.py"], check=True)
                print("app.py executed successfully.")
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