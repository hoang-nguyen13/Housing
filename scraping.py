import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import gc

def scrape_district(district):
    print(f"Scraping listings for {district}...")

    # Set up WebDriver
    service = Service('/usr/local/bin/chromedriver')  # Update with your correct path
    driver = webdriver.Chrome(service=service)

    first_page_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}?cIds=650"
    paginated_url = f"https://batdongsan.com.vn/ban-can-ho-chung-cu-{district}/p{{}}?cIds=650"

    page = 1  # Start from page 1
    district_data = []  # Temporary list to store data for the current district

    while True:
        # Set URL format correctly
        url = first_page_url if page == 1 else paginated_url.format(page)
        print(f"Scraping page {page}... {url}")

        # Load the page
        driver.get(url)
        time.sleep(5)  # Wait for JavaScript to load content

        # Get HTML content
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # **Detect if the error page is shown**
        error_message = soup.find("div", class_="error-content")  # Adjust class based on the actual error page
        if error_message or "404" in driver.title or "Không có kết quả nào phù hợp" in soup.text:
            print("Error page detected. Stopping...")
            break

        # Find all property listings
        listings = soup.find_all("div", class_="re__card-info")
        if not listings:  # If no listings found, stop scraping
            print("No more listings found. Stopping...")
            break

        for listing in listings:
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

            # Append extracted data as a row with the district name
            district_data.append([product_title, price, area, price_per_m2, bedroom, toilet, location])

        # Move to the next page
        page += 1

    # After finishing scraping the current district, save the data to CSV
    df_district = pd.DataFrame(district_data, columns=["Product Title", "Price", "Area", "Price per m²", "Bedrooms", "Toilets", "Location"])

    # Filter out rows where everything except the title is 'N/A'
    df_filtered = df_district[~df_district.iloc[:, 1:].apply(lambda row: (row == 'N/A').all(), axis=1)]

    # Filter out rows where everything except the location is 'N/A'
    df_filtered = df_filtered[~df_filtered.iloc[:, :-1].apply(lambda row: (row == 'N/A').all(), axis=1)]

    # Save the filtered DataFrame for the current district to a CSV
    df_filtered.to_csv(f"data/filtered_real_estate_listings_{district}.csv", index=False)
    
    
    # Clear the district data to free up memory
    del district_data
    gc.collect()  # Trigger garbage collection to free up unused memory

    print(f"Finished scraping for {district}. Data saved.")

    # Quit Selenium WebDriver
    driver.quit()