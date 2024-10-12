from datetime import datetime

from service.extract_service.src.crawler.source_1_crawler import Source1Crawler
from service.extract_service.src.util.file_handler import write_json_to_csv


def run_crawlers():
    # Initialize Source1Crawler
    source1_crawler = Source1Crawler()
    source1_crawler.setup_driver(headless=True)  # Headless browser option
    # source1_crawler.setJwt()
    data = source1_crawler.crawl()
    current_date = datetime.now().strftime("%H_%M__%d_%m_%Y")
    filename = f"source_1_{current_date}.csv"
    write_json_to_csv(filename, data)


if __name__ == "__main__":
    print("Running crawlers...")
    run_crawlers()
