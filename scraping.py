import time
import psutil
import gc
import re
import threading
import csv
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


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

def print_memory_usage(stop_event):
    """
    Print memory usage every 10 seconds until stop_event is set.
    
    Args:
        stop_event (threading.Event): An event to stop the memory monitoring thread.
    returns:
        None
    """
    process = psutil.Process()  # Current process
    while not stop_event.is_set():
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / 1024 / 1024  # Resident Set Size in MB
        vms_mb = mem_info.vms / 1024 / 1024  # Virtual Memory Size in MB
        print(f"Memory Usage - RSS: {rss_mb:.2f} MB, VMS: {vms_mb:.2f} MB")
        time.sleep(10)  # Wait 10 seconds
    return None

def setup_driver():
    """
    Set up a Chrome WebDriver with options to avoid detection.
    
    Args:
        None
    Returns:
        driver (webdriver.Chrome): A Chrome WebDriver instance.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Keep headless for efficiency
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    # Add user-agent to mimic a real browser
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
    # Disable automation flags to avoid detection
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    service = Service('/usr/local/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # Hide WebDriver property
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver

def check_duplicate_in_csv(csv_file_path, product_id, date_element):
    """
    Check if a product_id and date_element pair exists in the CSV.
    Args:
        csv_file_path (str): The path to the CSV file.
        product_id (str): The product ID to check.
        date_element (str): The date element to check.
    Returns:
        bool: True if the pair exists, False otherwise.
    """
    if not os.path.exists(csv_file_path):
        return False  # No file, no duplicates
    
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if (row["Id"] == product_id and 
                row["Date Posted"] == date_element):
                return True
    return False


def scrape_district(district, start_page, end_page):
    #explain the function
    """
    Scrape real estate listings for a specific district on BatDongSan.com.vn
    and save the data to a CSV file.

    Args:
        district (str): The district to scrape listings for.

    Returns:
        None
    """
    print(f"Total pages: {end_page}")
    print(f"Scraping {district} from page {start_page} to {end_page}...")    
    # Set up WebDriver
    # service = Service('/usr/local/bin/chromedriver')  # Update with your correct path
    driver = setup_driver()

    first_page_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}"
    paginated_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}/p{{}}"

    page = 1  # Start from page 1
    district_data = []  # Temporary list to store data for the current district
    
        # Start memory monitoring in a separate thread
    stop_event = threading.Event()
    memory_thread = threading.Thread(target=print_memory_usage, args=(stop_event,))
    memory_thread.daemon = True  # Thread stops when main program exits
    memory_thread.start()
    
    # if the .csv file of filtered_real_estate_listings_district exists, then move on, if not create it
    csv_file_path = f"data/filtered_real_estate_listings_{district}.csv"
    if os.path.exists(csv_file_path):
        print(f"CSV file for {district} already exists. Skipping...")
        pass
    else:
        print(f"CSV file for {district} does not exist. Creating...")
        open(csv_file_path, 'w').close()
    
    try:  
        for page in range(start_page, end_page + 1):
            # Set URL format correctly
            url = first_page_url if page == 1 else paginated_url.format(page)
            print(f"Scraping page {page}... {url}")

            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "re__srp-list")))
            except TimeoutException as e:
                # Check page source to confirm end of pagination
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                if "Không có kết quả nào phù hợp" in soup.text or soup.find("div", class_="error-content"):
                    print(f"No more listings found beyond page {page-1} for {district}. Stopping...")
                else:
                    print(f"Timeout waiting for re__srp-list on {url}: {e} (Unexpected, debug saved)")
                    with open(f"debug_{district}_page_{page}.html", "w") as f:
                        f.write(page_source)
                break
            # Get HTML content
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")

            # **Detect if the error page is shown**
            error_message = soup.find("div", class_="error-content")  # Adjust class based on the actual error page
            if error_message or "404" in driver.title or "Không có kết quả nào phù hợp" in soup.text:
                print("Error page detected. Stopping...")
                break
            
            # Find the main listing container
            srp_list = soup.find("div", class_="re__srp-list")
            if not srp_list:
                print("No listing container found. Stopping...")
                break

            # Find all cards within the main container
            all_cards = srp_list.find_all("div", class_="js__card", recursive=True)
            # Filter out unwanted cards
            listings = []
            for card in all_cards:
                card_classes = " ".join(card.get("class", []))
                # Skip if it’s an ad or has prid="0"
                if ("promoted-ads-appearance-position" in card_classes or 
                    card.get("prid", "0") == "0"):
                    # print(f"Skipping ad or prid=0 card: {card_classes}")
                    continue
                # Skip if it’s inside the verified similar listings section
                if card.find_parent("div", class_="re__listing-verified-similar-v2"):
                    # print(f"Skipping verified similar listing card: {card_classes}")
                    continue
                # Skip promotional links
                product_link = card.find("a", class_="js__product-link-for-product-id")
                if product_link and "js__product-link-promotion-ads" in " ".join(product_link.get("class", [])):
                    # print(f"Skipping promotional ad link: {product_link.get('href', 'N/A')}")
                    continue
                listings.append(card)

            if not listings:
                print(f"No more listings found on page {page}. Stopping this process...")
                break

            for listing in listings:
                # Extract product_id from the <a> tag
                product_link = listing.find("a", class_="js__product-link-for-product-id")
                product_id = product_link["data-product-id"] if product_link else "N/A"
                # Extract other details from re__card-info
                
                info_div = listing.find("div", class_="re__card-info")
                if not info_div:
                    continue  # Skip if no info div found

                # Extract Date Listed
                date_element = listing.find("span", class_="re__card-published-info-published-at")
                date_element = date_element["aria-label"] if date_element else "N/A"
                # Extract Product Title
                product_title = listing.find("span", class_="pr-title js__card-title")
                product_title = product_title.text.strip() if product_title else "N/A"

                # Extract Price
                price = listing.find("span", class_="re__card-config-price js__card-config-item")
                price = price.text.strip() if price else "N/A"

                # Extract Area
                area = listing.find("span", class_="re__card-config-area js__card-config-item")
                area = area.text.strip() if area else "N/A"

                # Extract Price per m²
                price_per_m2 = listing.find("span", class_="re__card-config-price_per_m2 js__card-config-item")
                price_per_m2 = price_per_m2.text.strip() if price_per_m2 else "N/A"

                # Extract Number of Bedrooms
                bedroom = listing.find("span", class_="re__card-config-bedroom js__card-config-item")
                bedroom = bedroom.text.strip() if bedroom else "N/A"

                # Extract Number of Toilets
                toilet = listing.find("span", class_="re__card-config-toilet js__card-config-item")
                toilet = toilet.text.strip() if toilet else "N/A"

                # Extract Location
                location = listing.find("div", class_="re__card-location")
                location = location.find("span").text.strip() if location else "N/A"
                
                # Step 1: Extract the href link
                link_element = listing.find("a", class_="js__product-link-for-product-id")
                href = link_element["href"] if link_element else None

                # Step 2: Navigate to the detail page and extract coordinates
                coordinates = "N/A"
                if href:
                    full_url = f"https://batdongsan.com.vn{href}"  # Prepend domain if href is relative
                    # print(f"Navigating to: {full_url}")
                    try:
                        driver.get(full_url)
                        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "lazyload")))
                        
                        detail_page_source = driver.page_source
                        detail_soup = BeautifulSoup(detail_page_source, "html.parser")
                        # Step 1: Find the coordinates section
                        iframe = detail_soup.find("iframe", class_="lazyload")
                        if iframe and "data-src" in iframe.attrs:
                            iframe_url = iframe["data-src"]
                            # Extract lat/lon from URL using regex
                            match = re.search(r"q=([-+]?\d+\.\d+),([-+]?\d+\.\d+)", iframe_url)
                            if match:
                                lat, lon = match.groups()
                                coordinates = f"{lat}, {lon}"  # e.g., "21.021807, 105.857699"
                                # print(f"Coordinates found: {coordinates}")
                            else:
                                print(f"No coordinates found in iframe URL: {iframe_url}")
                        else:
                            print("No iframe found on detail page")
                        del detail_page_source, detail_soup, iframe, iframe_url
                        gc.collect()  # Force garbage collection after detail page
                    except TimeoutException as e:
                        print(f"Timeout waiting for lazyload on {full_url}: {e}")
                # Append extracted data as a row with the district name
                
                 # Check for duplicates in CSV
                if check_duplicate_in_csv(csv_file_path, product_id, date_element):
                    print(f"Skipping duplicate entry from CSV: Product ID {product_id}, Date {date_element}")
                    continue

                district_data.append([product_id, date_element, product_title, price, area, price_per_m2, bedroom, toilet, location, coordinates])
            
            # Save data incrementally after each page
            if district_data:
                df_page = pd.DataFrame(district_data, columns=[
                    "Id", "Date Posted", "Product Title", "Price", "Area", "Price per m²", 
                    "Bedrooms", "Toilets", "Location", "Coordinates"
                ])
                mode = "w" if page == 1 else "a"  # Write header on first page, append later
                df_page.to_csv(f"data/filtered_real_estate_listings_{district}.csv", mode=mode, header=(page == 1), index=False)
                print(f"Data saved for page {page}.")
                del df_page, district_data
                district_data = []  # Reset list

            # Clean up page-level objects
            del page_source, soup, srp_list, all_cards, listings
            gc.collect()  # Force garbage collection
            page += 1
    finally:
        # Clear the district data to free up memory
        # Stop memory monitoring and clean up
        stop_event.set()  # Signal the thread to stop
        memory_thread.join()  # Wait for the thread to finish
        del district_data
        gc.collect()  # Trigger garbage collection to free up unused memory
        # Quit Selenium WebDriver
        driver.quit()
        print(f"Finished scraping for {district}. Data saved.")
    return None