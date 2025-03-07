from multiprocessing import Pool, Value  # Import Pool for parallel tasks and Value for shared counter
from scraping import districts, scrape_district  # Import districts list and scraping function
from fetch_parse import process_real_estate_data  # Import data processing function
from tqdm import tqdm  # Import tqdm for progress bars
import sys  # Import sys for output control

def init_counter(counter):
    """Initialize the shared counter for multiprocessing."""
    # Set a global variable for the counter in each worker process
    # global: Makes the variable usable everywhere, district_counter: The shared counter
    global district_counter
    district_counter = counter

def scrape_with_counter(district):
    """Wrapper function to scrape a district and increment the counter."""
    # Run the scraping function for this district
    # scrape_district(): The function from scraping.py, district: The current district
    scrape_district(district)
    # Safely increase the counter after scraping is done
    # with: Ensures safe access, district_counter: The shared counter, 
    # .get_lock(): Prevents conflicts, .value: The counter’s value
    with district_counter.get_lock():
        district_counter.value += 1

def main():
    """
    Main function to scrape real estate listings for all districts in Hanoi
    and process the data.
    """
    # Set the total number of districts to scrape (12 in this case)
    # total_districts: A count of how many districts we’re working with, len(): Counts items
    total_districts = len(districts)
    # Create a shared counter to track completed districts across processes
    # counter: A shared number, Value(): Makes it sharable, 'i': Integer type, 0: Starting value
    counter = Value('i', 0)

    # Start a section to run the main tasks and catch problems
    # try: Tests the main code and handles errors
    try:
        # Ask the user if they want to start scraping and clean their input
        # user_input: User’s answer, input(): Gets text, .strip(): Removes spaces, .lower(): All lowercase
        user_input = input("Do you want to start scraping? (yes/no): ").strip().lower()
        
        # Check if the user agreed to start scraping
        # if: Tests condition, user_input: User’s answer, ==: Matches "yes"
        if user_input == 'yes':
            # Show a message that scraping is starting
            # print(): Displays text, "Starting scraping...": The message
            print("Starting scraping...")
            
            # Create a pool of 6 workers with the shared counter initialized
            # Pool: Runs tasks in parallel, processes=6: 6 workers, 
            # initializer=init_counter: Sets up counter, initargs=(counter,): Passes counter
            with Pool(processes=12, initializer=init_counter, initargs=(counter,)) as pool:
                # Start a section to handle scraping and catch interruptions
                # try: Tests scraping and catches problems
                try:
                    # Run scraping tasks in parallel and get results as they finish
                    # results: Iterator of task completions, pool: Worker pool, 
                    # .imap_unordered(): Runs tasks out of order, scrape_with_counter: Wrapped function
                    results = pool.imap_unordered(scrape_with_counter, districts)
                    
                    # Show a progress bar for all districts
                    # for: Loops over results, _: Ignores result value, 
                    # tqdm(): Makes progress bar, results: Task completions, 
                    # total=total_districts: Total tasks (12), desc="Scraping Districts": Label, 
                    # unit="district": Progress unit, file=sys.stdout: Output location
                    for _ in tqdm(results, total=total_districts, desc="Scraping Districts", unit="district", file=sys.stdout):
                        pass  # Just wait for results since data is saved in scrape_district
                    
                    # Show a final message with all districts completed
                    # print(): Displays text, f"": Makes string, total_districts: Total count
                    print(f"\nNumber of districts scraped: {total_districts}/{total_districts}")
                    # Confirm scraping is fully done
                    # print(): Displays text, "Scraping completed...": Message
                    print("Scraping completed for all districts.")
                
                # Catch if the user stops with Ctrl+C
                # except: Handles interruption, KeyboardInterrupt: User stop
                except KeyboardInterrupt:
                    # Show a message that scraping was interrupted
                    # print(): Displays text, "\n": New line, "Scraping interrupted...": Message
                    print("\nScraping interrupted! Cleaning up...")
                    # Stop all workers immediately
                    # pool: Worker pool, .terminate(): Stops tasks
                    pool.terminate()
                    # Wait for workers to finish stopping
                    # pool: Worker pool, .join(): Waits until done
                    pool.join()
                    # Show cleanup is complete
                    # print(): Displays text, "Scraping process terminated...": Message
                    print("Scraping process terminated safely.")
                    # Exit to stop further steps
                    # return: Ends function
                    return
                
                # Catch other scraping errors
                # except: Handles errors, Exception: Any problem, e: Error details
                except Exception as e:
                    # Show error message with details
                    # print(): Displays text, f"": Makes string, e: Error info
                    print(f"\nAn error occurred during scraping: {e}")
                    # Close the pool normally
                    # pool: Worker pool, .close(): Stops new tasks
                    pool.close()
                    # Wait for workers to finish
                    # pool: Worker pool, .join(): Waits until done
                    pool.join()
                    # Exit to stop further steps
                    # return: Ends function
                    return

        # If user didn’t say "yes," skip scraping
        # else: Runs if "if" fails
        else:
            # Show message that scraping was skipped
            # print(): Displays text, "Scraping skipped.": Message
            print("Scraping skipped.")

        # Ask if the user wants to parse data
        # user_input: User’s answer, input(): Gets text, .strip(): Removes spaces, .lower(): All lowercase
        user_input = input("Do you want to start parsing? (yes/no): ").strip().lower()
        
        # Check if user agreed to parse
        # if: Tests condition, user_input: User’s answer, ==: Matches "yes"
        if user_input == 'yes':
            # Show message that parsing is starting
            # print(): Displays text, "Starting parsing...": Message
            print("Starting parsing...")
            # Run the parsing function
            # process_real_estate_data(): Function from fetch_parse.py
            process_real_estate_data()
            # Show message that parsing is done
            # print(): Displays text, "Parsing completed...": Message
            print("Parsing completed for all districts.")
        
        # If user didn’t say "yes," skip parsing
        # else: Runs if "if" fails
        else:
            # Show message that parsing was skipped
            # print(): Displays text, "Parsing skipped.": Message
            print("Parsing skipped.")

    # Catch user interruption outside the pool
    # except: Handles interruption, KeyboardInterrupt: User stop
    except KeyboardInterrupt:
        # Show message that process was stopped
        # print(): Displays text, "\n": New line, "Process interrupted...": Message
        print("\nProcess interrupted! Exiting safely.")
    
    # Catch unexpected errors
    # except: Handles errors, Exception: Any problem, e: Error details
    except Exception as e:
        # Show error message with details
        # print(): Displays text, f"": Makes string, e: Error info
        print(f"\nAn unexpected error occurred: {e}")

# Run the script if it’s the main file
# if: Tests condition, __name__: Special variable, "__main__": Value when run directly
if __name__ == "__main__":
    # Start the main function
    # main(): Runs the program
    main()