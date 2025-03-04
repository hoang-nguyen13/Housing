import time
import psutil
import threading
import csv
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.safari.options import Options as SafariOptions
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException
from bs4 import BeautifulSoup
import re
import gc

districts = [
    "cau-giay", "nam-tu-liem", "bac-tu-liem", "hai-ba-trung",
    "dong-da", "ha-dong", "hoang-mai", "long-bien",
    "tay-ho", "ba-dinh"
]

# Pre-patched ChromeDriver path
CHROMEDRIVER_PATH = "/Users/hoangnguyen/Library/Application Support/undetected_chromedriver/undetected_chromedriver"

def print_memory_usage(stop_event):
    process = psutil.Process()
    while not stop_event.is_set():
        mem_info = process.memory_info()
        print(f"Memory Usage - RSS: {mem_info.rss / 1024 / 1024:.2f} MB")
        time.sleep(120)

def setup_driver(use_chrome=False):
    """
    Set up either a Safari or undetected_chromedriver WebDriver.

    Args:
        use_chrome (bool): If True, use undetected_chromedriver; if False, use Safari.
    Returns:
        WebDriver instance
    """
    if use_chrome:
        options = uc.ChromeOptions()
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=800,600")
        options.add_argument("user-data-dir=./chrome_profile")  # Persistent profile
        driver = uc.Chrome(options=options, driver_executable_path=CHROMEDRIVER_PATH, use_subprocess=True)
        print(f"PID {os.getpid()} - Undetected ChromeDriver initialized")
    else:
        options = SafariOptions()
        driver = webdriver.Safari(options=options)
        driver.set_window_size(800, 600)  # Smaller window size
        print(f"PID {os.getpid()} - Safari driver initialized")
    return driver

def check_duplicate_in_csv(csv_file_path, product_id, date_element):
    if not os.path.exists(csv_file_path):
        return False
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        return any(row["Id"] == product_id and row["Date Posted"] == date_element for row in csv.DictReader(csvfile))

def pre_patch_chromedriver():
    """Ensure ChromeDriver is patched before scraping."""
    if not os.path.exists(CHROMEDRIVER_PATH):
        print("Pre-patching ChromeDriver...")
        driver = uc.Chrome()  # This will download and patch the binary
        driver.quit()
    else:
        print("ChromeDriver already patched at", CHROMEDRIVER_PATH)

def scrape_district(district, use_chrome=False):
    """
    Scrape real estate listings for a specific district on BatDongSan.com.vn
    and save the data to a CSV file, including coordinates from detail pages.

    Args:
        district (str): The district to scrape listings for.
        use_chrome (bool): If True, use undetected_chromedriver; if False, use Safari.
    Returns:
        None
    """
    print(f"Scraping {district} with {'Undetected Chrome' if use_chrome else 'Safari'}...")    
    # Set up WebDriver
    driver = setup_driver(use_chrome=use_chrome)

    first_page_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}"
    paginated_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}/p{{}}"

    page = 1  # Start from page 1
    district_data = []  # Temporary list to store data for the current district
    
    # Start memory monitoring in a separate thread
    stop_event = threading.Event()
    memory_thread = threading.Thread(target=print_memory_usage, args=(stop_event,))
    memory_thread.daemon = True  # Thread stops when main program exits
    memory_thread.start()
    
    # If the .csv file of filtered_real_estate_listings_district exists, then move on, if not create it
    csv_file_path = f"data/filtered_real_estate_listings_{district}.csv"
    if os.path.exists(csv_file_path):
        print(f"CSV file for {district} already exists. Skipping...")
    else:
        print(f"CSV file for {district} does not exist. Creating...")
        open(csv_file_path, 'w').close()
    
    try:  
        while True:  # Loop until break condition is met
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

            # Detect CAPTCHA for Chrome
            if use_chrome and ("verify you are human" in page_source.lower() or soup.find("div", id="challenge-form")):
                print(f"CAPTCHA detected on {url}. Please solve it manually in Chrome.")
                input(f"Solve the CAPTCHA in Chrome for {district}, then press Enter to continue...")
                try:
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "re__srp-list")))
                    page_source = driver.page_source  # Refresh page source after CAPTCHA
                    soup = BeautifulSoup(page_source, "html.parser")
                except TimeoutException:
                    print(f"Still couldn’t load {url} after CAPTCHA. Stopping.")
                    break

            # **Detect if the error page is shown**
            error_message = soup.find("div", class_="error-content")
            if error_message or "404" in driver.title or "Không có kết quả nào phù hợp" in soup.text:
                print("Error page detected. Stopping...")
                break
            
            # Find the main listing container
            srp_list = soup.find("div", class_="re__srp-list")
            if not srp_list:
                print("No listing container found. Stopping...")
                break

            # Find all valid listings
            listings = [card for card in srp_list.find_all("div", class_="js__card", recursive=True)
                        if "promoted-ads-appearance-position" not in " ".join(card.get("class", []))
                        and card.get("prid", "0") != "0"
                        and not card.find_parent("div", class_="re__listing-verified-similar-v2")]

            if not listings:
                print(f"No more listings found on page {page}. Stopping this process...")
                break

            for listing in listings:
                # Extract product_id from the <a> tag
                product_link = listing.find("a", class_="js__product-link-for-product-id")
                product_id = product_link["data-product-id"] if product_link else "N/A"
                
                info_div = listing.find("div", class_="re__card-info")
                if not info_div:
                    continue  # Skip if no info div found
                
                date_element = listing.find("span", class_="re__card-published-info-published-at")
                date_element = date_element["aria-label"] if date_element else "N/A"
                
                product_title = listing.find("span", class_="pr-title js__card-title")
                product_title = product_title.text.strip() if product_title else "N/A"
                
                if check_duplicate_in_csv(csv_file_path, product_id, date_element):
                    print(f"Skipping duplicate entry from CSV: Product ID {product_id}, Date {date_element}")
                    continue

                # Extract other details
                details = {
                    "Price": "re__card-config-price js__card-config-item",
                    "Area": "re__card-config-area js__card-config-item",
                    "Price per m²": "re__card-config-price_per_m2 js__card-config-item",
                    "Bedrooms": "re__card-config-bedroom js__card-config-item",
                    "Toilets": "re__card-config-toilet js__card-config-item",
                    "Location": "re__card-location",
                }
                extracted_details = {key: (listing.find("span", class_=value).text.strip() if listing.find("span", class_=value) else "N/A") for key, value in details.items()}
                
                # Extract href and get coordinates from detail page
                href = product_link["href"] if product_link else "N/A"
                full_href = f"https://batdongsan.com.vn{href}" if href != "N/A" else "N/A"
                coordinates = "N/A"
                if full_href != "N/A":
                    try:
                        print(f"Navigating to detail page: {full_href}")
                        driver.get(full_href)
                        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "lazyload")))
                        detail_soup = BeautifulSoup(driver.page_source, "html.parser")
                        iframe = detail_soup.find("iframe", class_="lazyload")
                        if iframe and "data-src" in iframe.attrs:
                            match = re.search(r"q=([-+]?\d+\.\d+),([-+]?\d+\.\d+)", iframe["data-src"])
                            if match:
                                coordinates = f"{match.group(1)}, {match.group(2)}"
                        print(f"Coordinates extracted for Product ID {product_id}: {coordinates}")
                    except (TimeoutException, NoSuchWindowException) as e:
                        print(f"Detail page error for {full_href}: {e}. Coordinates set to N/A.")
                
                district_data.append([product_id, date_element, product_title] + list(extracted_details.values()) + [coordinates])
                print(f"Added listing: Product ID {product_id}, Coordinates: {coordinates}")
            
            # Save data incrementally after each page
            if district_data:
                df_page = pd.DataFrame(district_data, columns=["Id", "Date Posted", "Product Title"] + list(details.keys()) + ["Coordinates"])
                df_page.to_csv(csv_file_path, mode="a", header=(page == 1), index=False)
                print(f"Data saved for page {page}: {len(district_data)} listings.")
                district_data = []  # Reset list
            
            gc.collect()
            page += 1  # Move to next page

    finally:
        stop_event.set()
        memory_thread.join(timeout=1)  # Give thread a second to finish
        driver.quit()
        print(f"Finished scraping for {district}. Data saved.")
    return None