from datetime import datetime

from selenium.common import WebDriverException

from service.extract_service.src.crawler.paging_base_crawler import PagingBase
from service.extract_service.src.util.file_util import write_json_to_csv, write_json_to_file


class SourceACrawler(PagingBase):
    def __init__(self, limit_page, base_url, domain):
        super().__init__(limit_page)
        self._domain = domain
        self._base_url = base_url

    def crawl_page(self, page):
        url_page = f"{self._base_url}/p{page}"
        print(f"Visiting page: {url_page}")
        self.get_url(url_page)

        # Wait for 5 seconds
        self.wait(5)
        driver = self.driver.page_source
        self.filter_script(driver)

        estate_list = self.soup.select(".js__card")
        list_url = []
        for (estate) in estate_list:
            link = estate.select_one(".js__product-link-for-product-id").get("href")
            list_url.append(f"{self._domain}{str(link)}")
        return list_url

    def crawl_item(self, url):
        try:
            self.get_url(url)
        except WebDriverException as e:
            return None
        print(f"Visiting item: {url}")

        self.wait(10)
        current_url = self.driver.current_url
        if current_url != url:
            return None

        driver = self.driver.page_source
        self.filter_script(driver)

        title = self.soup.select_one(".pr-title").get_text(strip=True)
        address = self.soup.select_one(".js__pr-address").get_text(strip=True)
        description = self.soup.select_one(".re__detail-content").get_text(strip=True)
        images = self.soup.select(".slick-track img")
        result = {
            "Subject": title,
            "Address": address,
            "Description": description
        }
        properties = self.soup.select(".re__pr-specs-content-item")
        result["properties"] = {}
        result["images"] = []
        for item in images:
            result["images"].append(item.get("src"))
        for item in properties:
            key = item.select_one(".re__pr-specs-content-item-title").get_text(strip=True)
            value = item.select_one(".re__pr-specs-content-item-value").get_text(strip=True)
            result["properties"][key] = value

        seller = self.soup.select_one(".js__ob-agent-info")
        email_selector = seller.select_one("#email")
        avatar_selector = seller.select_one("img")
        result['agent'] = {
            'avatar': avatar_selector.get("src") if avatar_selector else None,
            'fullname': seller.select_one(".js_contact-name").get("title"),
            'email': email_selector.get("data-email") if email_selector else None,
        }

        result["created_at"] = datetime.now().strftime("%H:%M %d:%m:%Y")
        info = self.soup.select(".js__pr-config-item")
        result['start_date'] = info[0].select_one(".value").get_text(strip=True)
        result['end_date'] = info[1].select_one(".value").get_text(strip=True)
        result["src"] = current_url
        result["natural_id"] = info[3].select_one(".value").get_text(strip=True)
        return result

    def after_run(self):
        data = self._list_item
        current_date = datetime.now().strftime("%Y_%m_%d__%H_%M")
        print(data)
        write_json_to_file(f"source_1_{current_date}.json", data)
        filename = f"source_1_{current_date}.csv"
        write_json_to_csv(filename, data)
        print(f"Data has been saved to {filename}")

    def handle_error_item(self, error):
        super().handle_error_item(error)

    @property
    def base_url(self):
        return self._base_url