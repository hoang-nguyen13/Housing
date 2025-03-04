from scraping import scrape_district, districts
import time
def main():
    response = input("Do you want to start scraping? (yes/no): ").strip().lower()
    if response == "yes":
        for district in districts:
            scrape_district(district)
            print(f"Completed {district}. Moving to next district...")
            time.sleep(10)  # Delay between districts
    else:
        print("Scraping aborted.")

if __name__ == "__main__":
    main()