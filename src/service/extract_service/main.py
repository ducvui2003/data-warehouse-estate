from src.service.controller_service.crawl_controller import CrawlController

# > python -m src.service.extract_service.main
if __name__ == '__main__':
    # 6. Crawl_Data:
    # 6.1. sử hàm getConfig của crawlController đã tạo trước đó
    crawl_controller = CrawlController()
    crawl_controller.get_config()
