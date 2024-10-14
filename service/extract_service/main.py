from datetime import datetime

from service.extract_service.src.config.setting import LIMIT_PAGE, SOURCE_A_BASE, SOURCE_A_1, SOURCE_A_2
from service.extract_service.src.crawler.source_A_crawler import SourceACrawler
from service.extract_service.src.util.file_util import write_json_to_csv


def run_crawlers():
    run_crawler_source_A_1()
    run_crawler_source_A_2()


def run_crawler_source_A_1():
    source_1_crawler = SourceACrawler(LIMIT_PAGE, SOURCE_A_BASE, SOURCE_A_1)
    print(f"Started crawl at: {source_1_crawler.base_url}")
    data = source_1_crawler.handle()
    current_date = datetime.now().strftime("%H_%M__%d_%m_%Y")
    filename = f"source_a_2_{current_date}.csv"
    write_json_to_csv(filename, data)


def run_crawler_source_A_2():
    source_a_2_crawler = SourceACrawler(LIMIT_PAGE, SOURCE_A_BASE, SOURCE_A_2)
    print(f"Started crawl at: {source_a_2_crawler.base_url}")
    data = source_a_2_crawler.handle()
    current_date = datetime.now().strftime("%H_%M__%d_%m_%Y")
    filename = f"source_a_2_{current_date}.csv"
    write_json_to_csv(filename, data)


if __name__ == "__main__":
    print("Running crawlers...")
    run_crawlers()
