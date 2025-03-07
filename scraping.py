# Import the time module for handling delays and timing
import time
# Import psutil for monitoring system resources like memory usage
import psutil
# Import threading for running memory monitoring in a separate thread
import threading
# Import csv for reading and writing CSV files
import csv
# Import os for interacting with the operating system (e.g., file existence checks)
import os
# Import re for regular expression operations (e.g., extracting coordinates)
import re
# Import pandas for data manipulation and CSV writing
import pandas as pd
# Import selenium.webdriver for web scraping with Chrome
from selenium import webdriver
# Import Service to specify the ChromeDriver executable path
from selenium.webdriver.chrome.service import Service
# Import Options to configure Chrome browser settings
from selenium.webdriver.chrome.options import Options
# Import BeautifulSoup for parsing HTML content
from bs4 import BeautifulSoup
# Import By for locating elements on a webpage
from selenium.webdriver.common.by import By
# Import WebDriverWait for waiting until page elements load
from selenium.webdriver.support.ui import WebDriverWait
# Import expected_conditions for defining conditions to wait for
from selenium.webdriver.support import expected_conditions as EC
# Import TimeoutException to handle timeouts during page loading
from selenium.common.exceptions import TimeoutException
# Import tqdm to show progress
from tqdm import tqdm
# Define a list of district names to scrape real estate data from
import gc
# Import the garbage collection module
from datetime import datetime
duplicate_cache = set()

districts = [
    "hoan-kiem",
    "thanh-xuan",
    "cau-giay",
    "nam-tu-liem",
    "bac-tu-liem",
    "hai-ba-trung",
    "dong-da",
    "ha-dong",
    "hoang-mai",
    "long-bien",
    "tay-ho",
    "ba-dinh",
]
# Define a function to print memory usage periodically until stopped
def print_memory_usage(stop_event):
    # Get the current process for monitoring its resource usage
    process = psutil.Process()
    # Loop until the stop_event is set
    while not stop_event.is_set():
        # Get memory information for the current process
        mem_info = process.memory_info()
        # Convert resident set size (RSS) to megabytes
        rss_mb = mem_info.rss / 1024 / 1024
        # Convert virtual memory size (VMS) to megabytes
        vms_mb = mem_info.vms / 1024 / 1024
        # Print the memory usage in RSS and VMS
        print(f"Memory Usage - RSS: {rss_mb:.2f} MB, VMS: {vms_mb:.2f} MB")
        # Pause execution for 120 seconds before the next check
        time.sleep(480)
    # Return None when the function ends (implicit return kept as per original)
    return None
# Define a function to create a tool (WebDriver) for controlling Chrome automatically
def setup_driver():
    # Make an object to set up Chrome's settings
    chrome_options = Options()
    # Tell Chrome to run without showing its window (headless means no visible browser)
    chrome_options.add_argument("--headless")
    # Turn off the graphics processor (GPU) to avoid problems when there's no screen
    chrome_options.add_argument("--disable-gpu")
    # Run Chrome without a "sandbox" (sandboxing is a security box that limits what Chrome can do)
    chrome_options.add_argument("--no-sandbox")
    # Stop Chrome from using a shared memory space to avoid crashes on some systems
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Turn off extra features (extensions) that Chrome might add, to keep it simple
    chrome_options.add_argument("--disable-extensions")
    # Pretend to be a normal user by setting a fake identity (user-agent) for Chrome
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
    # Hide signs that this is an automated Chrome (blink features are parts websites can check)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # Remove a clue (enable-automation) that websites use to spot automation
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # Turn off an extra automation tool so websites don’t notice we’re not human
    chrome_options.add_experimental_option("useAutomationExtension", False)
    # Point to the ChromeDriver file (a helper program) at this location
    service = Service('/usr/local/bin/chromedriver')
    # Start Chrome with our settings and the helper program
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # Run a trick to hide that we’re using automation (changes a setting websites might check)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    # Give back the ready-to-use Chrome controller
    return driver

def load_csv_to_cache(csv_file_path_merged):
    global duplicate_cache
    if os.path.exists(csv_file_path_merged):
        with open(csv_file_path_merged, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # print(f"CSV Headers: {reader.fieldnames}")
            initial_size = len(duplicate_cache)
            sample_entries = []
            for i, row in enumerate(reader):
                # Check if required fields exist and are non-empty
                if "Id" in row and "Date Posted" in row and row["Id"] and row["Date Posted"]:
                    entry = (str(row["Id"]).strip(), str(row["Date Posted"]).strip())
                    duplicate_cache.add(entry)
                    if i < 5:
                        sample_entries.append(entry)
                else:
                    print(f"Skipping invalid row: {row}")
            # print(f"Loaded {len(duplicate_cache) - initial_size} entries into duplicate_cache")
            # print(f"Total cache size: {len(duplicate_cache)}")
            # if sample_entries:
                # print("Sample cache entries:", sample_entries)
    else:
        print(f"CSV file not found at {csv_file_path_merged}")
def check_duplicate_in_csv(product_id, date_element):
    # Create the tuple to check
    check_tuple = (product_id, date_element)
    # Debugging output
    is_duplicate = check_tuple in duplicate_cache
    if is_duplicate:
        print(f"Duplicate found: ID={product_id}, Date={date_element}")
    # else:
    #     print(f"New entry: ID={product_id}, Date={date_element}")
    
    return is_duplicate
# Define a function to get location coordinates from a webpage
# (This sets up a way to find map numbers from a webpage link)
def fetch_coordinates(driver, full_href, product_id):
    # Set coordinates to "N/A" to start so we have something if we fail
    # coordinates: A place to store the map numbers, "N/A" means not found yet
    coordinates = "N/A"
    # Start a section that tries to do something and catches any mistakes
    # try: A way to test code and handle problems if they happen
    try:
        # Tell the browser tool to visit the webpage link we give it
        # driver: The browser controller, .get(): Makes it go to the link, 
        # full_href: The webpage address
        driver.get(full_href)
        # Wait up to 45 seconds for a special part of the page to show up
        # WebDriverWait: A waiting tool, driver: The browser it watches, 
        # 45: Seconds it waits, .until(): Waits until ready, 
        # EC: Rules for what’s ready, .presence_of_element_located(): Checks if a part is there, 
        # By.CLASS_NAME: Finds it by a label, "lazyload": A tag for a slow-loading part
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.CLASS_NAME, "lazyload")))
        # Grab all the webpage code once it’s loaded
        # detail_source: Holds the page code, driver: The browser tool, 
        # .page_source: Gets all the webpage text
        detail_source = driver.page_source
        # Use a tool to read and understand the webpage code
        # detail_soup: The readable version, BeautifulSoup: A code-reading tool, 
        # detail_source: The page code, "html.parser": How it reads the code
        detail_soup = BeautifulSoup(detail_source, "html.parser")
        # Look for a map box on the page with a certain label
        # iframe: The map box found, detail_soup: The readable page, 
        # .find(): Searches for something, "iframe": A mini webpage box, 
        # class_="lazyload": The label it has
        iframe = detail_soup.find("iframe", class_="lazyload")
        # Check if the map box exists and has a hidden link inside
        # if: Tests a condition, iframe: The map box, "data-src": A hidden link inside it, 
        # .attrs: The box’s details
        if iframe and "data-src" in iframe.attrs:
            # Use a search trick to find map numbers in the hidden link
            # match: The found numbers, re: A search tool, .search(): Looks for a pattern, 
            # r"q=([-+]?\d+\.\d+),([-+]?\d+\.\d+)": The pattern for map numbers, 
            # iframe["data-src"]: The hidden link text
            match = re.search(r"q=([-+]?\d+\.\d+),([-+]?\d+\.\d+)", iframe["data-src"])
            # If numbers are found, put them together as one string
            # if: Tests a condition, match: The found numbers, coordinates: Where we store them, 
            # f"": Makes a string, .group(1): First number (latitude), 
            # .group(2): Second number (longitude)
            if match:
                coordinates = f"{match.group(1)}, {match.group(2)}"
        
        # Clean up large objects after coordinates are obtained
        # del: Removes variables, detail_source, detail_soup, iframe, match: Objects no longer needed
        del detail_source
        del detail_soup
        if 'iframe' in locals():
            del iframe
        if 'match' in locals():
            del match
        # Force garbage collection to free up memory
        # gc: Garbage collector tool, .collect(): Runs cleanup
        gc.collect()

    # Catch any problems that happen while trying
    # except: Handles mistakes, Exception: Any problem, e: The problem’s details
    except Exception as e:
        # Show an error message if something goes wrong
        # print(): Shows a message, f"": Makes string, full_href: The webpage link, 
        # type(e): The problem type, .__name__: Its name, str(e): What happened
        print(f"Failed to load {full_href}: {type(e).__name__}: {str(e)}. Coordinates set to N/A.")
        # Clean up in case of failure (if objects were created)
        # del: Removes variables if they exist, locals(): Checks current scope
        if 'detail_source' in locals():
            del detail_source
        if 'detail_soup' in locals():
            del detail_soup
        if 'iframe' in locals():
            del iframe
        if 'match' in locals():
            del match
        # Force garbage collection after failure
        # gc: Garbage collector tool, .collect(): Runs cleanup
        gc.collect()

    # Give back the coordinates we found or "N/A" if we failed
    # return: Sends back the result, coordinates: The map numbers or "N/A"
    return coordinates
# Define a function to scrape real estate listings for a district
def scrape_district(district):
    print("Scraping for", district)
    # Set up a new WebDriver instance for this district
    driver = setup_driver()
    # Define the URL for the first page of listings
    first_page_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}"
    # Define the URL template for paginated pages
    paginated_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}/p{{}}"
    # Initialize the page counter starting at 1
    page = 1
    # Initialize a list to store district data
    district_data = []
    # Define the headers for the CSV file
    headers = ["Id", "Date Posted", "Product Title", "Price", "Area", "Price per m²", "Bedrooms", "Toilets", "Location", "Coordinates"]
    # Get current month abbreviation and year
    current_date = datetime.now()
    month_abbr = current_date.strftime('%b').lower()  # 'mar' for March
    year = current_date.strftime('%Y')  # '2025'
    # Create the directory name
    data_dir = f"data/{month_abbr}_{year}"
    data_dir_relevance = f"data/relevance"
    # Create the directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(data_dir_relevance, exist_ok=True)
    # Update the file paths
    csv_file_path = os.path.join(data_dir, f"filtered_real_estate_listings_{district}.csv")
    csv_file_path_merged = os.path.join(data_dir_relevance, f"merged_real_estate_listings.csv")
    # Check if the CSV file does not exist
    if not os.path.exists(csv_file_path):
        # Open the CSV file in write mode with UTF-8 encoding
        with open(csv_file_path, 'w', encoding='utf-8', newline='') as f:
            # Create a CSV writer object
            writer = csv.writer(f)
            # Write the headers to the CSV file
            writer.writerow(headers)
    # Create an event object to signal the memory thread to stop
    # stop_event: A switch to tell another task when to stop, 
    # threading: A tool for running tasks at the same time, 
    # .Event(): Makes the switch
    stop_event = threading.Event()
    # Create a thread to monitor memory usage
    # memory_thread: A helper task, threading: The tool for tasks, 
    # .Thread(): Sets up the task, target=print_memory_usage: What it does, 
    # args=(stop_event,): Gives it the stop switch
    memory_thread = threading.Thread(target=print_memory_usage, args=(stop_event,))
    # Set the memory thread as a daemon so it stops when the main thread exits
    # memory_thread: The helper task, .daemon: A setting to make it a background helper, 
    # True: Means it stops when the main program stops
    memory_thread.daemon = True
    # Start the memory monitoring thread
    # memory_thread: The helper task, .start(): Tells it to begin running
    memory_thread.start()
    # Begin a try block to handle scraping and ensure cleanup
    # try: Starts a section to test scraping and catch problems
    load_csv_to_cache(csv_file_path_merged)
    try:  
        # Load the first page to find the maximum page number (for progress bar estimation)
        # driver: Browser tool, .get(): Goes to the link, first_page_url: First page link
        driver.get(first_page_url)
        # Get the page source after loading
        # page_source: Page code, driver: Browser tool, .page_source: Grabs all text
        page_source = driver.page_source
        # Parse the page source with BeautifulSoup
        # soup: Readable page, BeautifulSoup: Code-reading tool, 
        # page_source: Page code, "html.parser": How it reads
        soup = BeautifulSoup(page_source, "html.parser")
        # Find the pagination section in the parsed HTML
        # pagination: Pagination box, soup: Readable page, .find(): Searches, 
        # "div": Page box, class_="re__pagination-group": Pagination label
        pagination = soup.find("div", class_="re__pagination-group")
        # Find the "Next" button
        # next_button: Next link, pagination: Pagination box, .find(): Searches, 
        # "a": Link tag, class_="re__pagination-icon": Next button label
        next_button = pagination.find("a", class_="re__pagination-icon") if pagination else None
        # Default max_page to 1 if no pagination or next button
        max_page = 1
        if next_button:
            # Get the previous sibling that’s an <a> with class="re__pagination-number"
            # prev_sibling: Element before next_button, .find_previous(): Looks backward, 
            # "a": Link tag, class_="re__pagination-number": Page number label
            last_page_link = next_button.find_previous("a", class_="re__pagination-number")
            if last_page_link and "pid" in last_page_link.attrs:
                # Extract the pid as the max page
                # max_page: Highest page, int(): Turns text to number, last_page_link["pid"]: Page ID
                max_page = int(last_page_link["pid"])
        # Print the estimated max page and initialize the progress bar
        print("Estimated max page for", district, "is", max_page)
        # tqdm(): Makes progress bar, total=max_page: Estimated total pages, 
        # desc: Bar label, unit="page": Unit of progress
        pbar = tqdm(total=max_page, desc=f"Scraping {district}", unit="page")

        # Start an infinite loop to scrape pages dynamically
        while True:
            # Set the URL to the first page or a paginated page based on the page number
            # url: Webpage link, first_page_url: First page link, page: Current page, 
            # paginated_url.format(page): Makes link for later pages
            url = first_page_url if page == 1 else paginated_url.format(page)
            # Begin a try block to handle page loading
            # try: Tests if the page loads without issues
            retries = 1
            while True:
                try:
                    driver.get(url)
                    WebDriverWait(driver, 60).until(
                        lambda driver: driver.find_elements(By.CLASS_NAME, "re__srp-list") or 
                                       driver.find_elements(By.CLASS_NAME, "re__srp-empty")
                    )
                    break
                except TimeoutException:
                    if retries > 0:
                        print(f"Timeout on page {page}. Retrying (attempts left: {retries})...")
                        retries -= 1
                        continue
                    else:
                        print(f"Timeout on page {page} after retry. Stopping.")
                        break  # Exit outer while True loop
            if retries == 0:  # If we broke due to timeout after retry
                break
            page_source = driver.page_source
            # Parse the page source with BeautifulSoup
            # soup: Readable page, BeautifulSoup: Code-reading tool, 
            # page_source: Page code, "html.parser": How it reads
            soup = BeautifulSoup(page_source, "html.parser")
            # Check for the "no results" div to stop scraping
            empty_results = soup.find("div", class_="re__srp-empty")
            if empty_results and "Không có kết quả nào phù hợp" in empty_results.get_text():
                print(f"No more results found on page {page}. Stopping scraping for {district}.")
                break

            # Check for listings and stop if none found
            srp_list = soup.find("div", class_="re__srp-list")
            if not srp_list:
                print(f"No listings on page {page}. Assuming end of results.")
                break
            # Find the listing container div in the parsed HTML
            # srp_list: Listing section, soup: Readable page, .find(): Searches, 
            # "div": Page box, class_="re__srp-list": Listing label
            srp_list = soup.find("div", class_="re__srp-list")
            # Find all listing cards within the container
            # all_cards: List of cards, srp_list: Listing section, .find_all(): Finds all, 
            # "div": Page box, class_="js__card": Card label, recursive=True: Looks deep
            all_cards = srp_list.find_all("div", class_="js__card", recursive=True)
            # Check each card, filter ads, and process valid listings
            # for: Loops over items, card: One card, all_cards: All cards found
            for card in all_cards:
                # Get the classes of the card as a space-separated string
                # card_classes: Card labels, " ".join(): Combines list, 
                # card.get(): Gets labels, "class": Label type, []: Default if none
                card_classes = " ".join(card.get("class", []))
                # Skip the card if it’s a promoted ad or has a prid of "0"
                # if: Tests condition, "promoted-ads-appearance-position": Ad label, 
                # card.get(): Gets prid, "prid": Ad ID, "0": Default or ad mark, 
                # continue: Skips this card
                if ("promoted-ads-appearance-position" in card_classes or card.get("prid", "0") == "0"):
                    continue
                # Skip the card if it’s under a verified similar listings section
                # if: Tests condition, card.find_parent(): Looks up parent, 
                # "div": Page box, class_="re__listing-verified-similar-v2": Similar section
                if card.find_parent("div", class_="re__listing-verified-similar-v2"):
                    continue
                # Find the product link within the card
                # product_link: Link found, card: Current card, .find(): Searches, 
                # "a": Link tag, class_="js__product-link-for-product-id": Link label
                product_link = card.find("a", class_="js__product-link-for-product-id")
                # Skip the card if the link is a promotion ad
                # if: Tests condition, product_link: The link, " ".join(): Combines classes, 
                # .get(): Gets classes, "class": Label type, []: Default, 
                # "js__product-link-promotion-ads": Promo label, continue: Skips card
                if product_link and "js__product-link-promotion-ads" in " ".join(product_link.get("class", [])):
                    continue
                # Extract the product ID from the link, default to "N/A" if not found
                # product_id: ID value, product_link: The link, 
                # ["data-product-id"]: ID tag, if: Tests if link exists, "N/A": Default
                product_id = product_link["data-product-id"] if product_link else "N/A"
                # Find the date element and extract its aria-label
                # date_element: Date found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-published-info-published-at": Date label
                date_element = card.find("span", class_="re__card-published-info-published-at")
                # Set date to "N/A" if not found, otherwise use the aria-label
                # date_element: Date value, ["aria-label"]: Date text, 
                # if: Tests if found, "N/A": Default
                date_element = date_element["aria-label"] if date_element else "N/A"
                # Check if the product ID and date are already in the CSV
                # if: Tests condition, check_duplicate_in_csv(): Checks file, 
                # csv_file_path: File location, product_id: ID, date_element: Date, 
                # continue: Skips if duplicate
                # print("checking id and data", product_id, date_element)
                if check_duplicate_in_csv(product_id, date_element):
                    # print("data skipped.")
                    continue
                # else:
                    # print("new data passed.")
                # Find the info div within the listing
                # info_div: Info section, card: Current card, .find(): Searches, 
                # "div": Page box, class_="re__card-info": Info label
                info_div = card.find("div", class_="re__card-info")
                # Skip the listing if no info div is found
                # if not: Tests if missing, info_div: Info section, continue: Skips card
                if not info_div:
                    continue
                # Find the product title span
                # product_title: Title found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="pr-title js__card-title": Title label
                product_title = card.find("span", class_="pr-title js__card-title")
                # Extract the title text, default to "N/A" if not found
                # product_title: Title value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                product_title = product_title.text.strip() if product_title else "N/A"
                # Find the price span
                # price: Price found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-config-price js__card-config-item": Price label
                price = card.find("span", class_="re__card-config-price js__card-config-item")
                # Extract the price text, default to "N/A" if not found
                # price: Price value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                price = price.text.strip() if price else "N/A"
                # Find the area span
                # area: Area found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-config-area js__card-config-item": Area label
                area = card.find("span", class_="re__card-config-area js__card-config-item")
                # Extract the area text, default to "N/A" if not found
                # area: Area value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                area = area.text.strip() if area else "N/A"
                # Find the price per square meter span
                # price_per_m2: Price/m² found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-config-price_per_m2 js__card-config-item": Price/m² label
                price_per_m2 = card.find("span", class_="re__card-config-price_per_m2 js__card-config-item")
                # Extract the price per m² text, default to "N/A" if not found
                # price_per_m2: Price/m² value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                price_per_m2 = price_per_m2.text.strip() if price_per_m2 else "N/A"
                # Find the bedroom span
                # bedroom: Bedrooms found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-config-bedroom js__card-config-item": Bedroom label
                bedroom = card.find("span", class_="re__card-config-bedroom js__card-config-item")
                # Extract the bedroom text, default to "N/A" if not found
                # bedroom: Bedroom value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                bedroom = bedroom.text.strip() if bedroom else "N/A"
                # Find the toilet span
                # toilet: Toilets found, card: Current card, .find(): Searches, 
                # "span": Text box, class_="re__card-config-toilet js__card-config-item": Toilet label
                toilet = card.find("span", class_="re__card-config-toilet js__card-config-item")
                # Extract the toilet text, default to "N/A" if not found
                # toilet: Toilet value, .text: Gets text, .strip(): Cleans it, 
                # if: Tests if found, "N/A": Default
                toilet = toilet.text.strip() if toilet else "N/A"
                # Find the location div
                # location: Location found, card: Current card, .find(): Searches, 
                # "div": Page box, class_="re__card-location": Location label
                location = card.find("div", class_="re__card-location")
                # Extract the location text from the first span, default to "N/A" if not found
                # location: Location value, .find(): Searches, "span": Text box, 
                # .text: Gets text, .strip(): Cleans it, if: Tests if found, "N/A": Default
                location = location.find("span").text.strip() if location else "N/A"
                # Get the href attribute from the product link
                # href: Link part, product_link: The link, ["href"]: Link text, 
                # if: Tests if found, "N/A": Default
                href = product_link["href"] if product_link else "N/A"
                # Construct the full URL by appending the href to the base URL
                # full_href: Full link, f"": Makes string, href: Link part, 
                # if: Tests if not "N/A", "N/A": Default
                full_href = f"https://batdongsan.com.vn{href}" if href != "N/A" else "N/A"
                # Fetch coordinates using the same tab
                # coordinates: Map numbers, fetch_coordinates(): Gets them, 
                # driver: Browser tool, full_href: Page link, product_id: ID
                coordinates = fetch_coordinates(driver, full_href, product_id)
                # Append the listing data to the district_data list
                # district_data: Data list, .append(): Adds to list, 
                # []: List of values for this listing
                district_data.append([product_id, date_element, product_title, price, area, 
                                     price_per_m2, bedroom, toilet, location, coordinates])
            # Check if there is data to save
            # if: Tests condition, district_data: Data list
            if district_data:
                # Create a DataFrame from the collected data with the specified headers
                # df_page: Data table, pd: Table tool, .DataFrame(): Makes table, 
                # district_data: Data list, columns=headers: Column names
                df_page = pd.DataFrame(district_data, columns=headers)
                # Append the DataFrame to the CSV file without headers
                # df_page: Data table, .to_csv(): Saves to file, csv_file_path: File location, 
                # mode="a": Adds to end, header=False: No extra titles, index=False: No row numbers
                df_page.to_csv(csv_file_path, mode="a", header=False, index=False)
                for entry in district_data:
                    duplicate_cache.add((entry[0], entry[1]))
                # Clear the district_data list for the next page
                # district_data: Data list, []: Empties it
                district_data = []
                # Explicitly delete df_page to remove reference
                # del: Removes variable, df_page: DataFrame to delete
                del df_page
                del page_source
                # Clear page source
                del soup
                # Clear beautiful soup object
                gc.collect()
                # Force garbage collection to free up memory
                # gc: Garbage collector tool, .collect(): Runs cleanup
            
            # Increment the page counter and update the progress bar
            page += 1
            pbar.update(1)

        # Close the progress bar when scraping is complete
        pbar.close()
    # Ensure cleanup happens even if an error occurs
    # finally: Runs no matter what to clean up
    finally:
        # Set the stop event to end the memory monitoring thread
        # stop_event: Stop switch, .set(): Turns it on
        stop_event.set()
        # Wait briefly for the memory thread to finish
        # memory_thread: Helper task, .join(1): Waits up to 1 second
        memory_thread.join(timeout=1)
        # Close the WebDriver instance immediately
        # driver: Browser tool, .quit(): Shuts it down
        driver.quit()
        # Print completion message
        # print(): Shows message, f"": Makes string, district: Area name
        print(f"Finished scraping for {district}. Data saved.")