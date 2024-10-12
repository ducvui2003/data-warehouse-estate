from datetime import datetime

from selenium.webdriver.support.wait import WebDriverWait

from selenium.webdriver.common.by import By

from service.crawler.src.config.setting import SOURCE_A_URL
from service.crawler.src.crawler.base_crawler import BaseCrawler


class Source1Crawler(BaseCrawler):
    _base_url = "https://batdongsan.com.vn"

    def setJwt(self):
        self.driver.get("https://batdongsan.com.vn/sellernet/trang-dang-nhap")
        WebDriverWait(self.driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")
        username_input = self.driver.find_element(By.CSS_SELECTOR, "input[name='username']")
        password_input = self.driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        # username_input.send_keys(USERNAME)
        # password_input.send_keys(PASSWORD)
        login_btn = self.driver.find_element(By.ID, "signin-button")
        login_btn.click()
        print("Logged in")
        self.wait(5)

    def crawl(self):
        self.get_url(SOURCE_A_URL)

        # Wait for 5 seconds
        self.wait(5)

        driver = self.driver.page_source
        self.filter_script(driver)

        estate_list = self.soup.select(".js__card")
        id_link = {}
        for (estate) in estate_list:
            natural_id = estate.select_one(".js__product-link-for-product-id").get('data-product-id')
            link = estate.select_one(".js__product-link-for-product-id").get("href")
            id_link[natural_id] = str(self._base_url + link)

        items = []
        for (natural_id, link) in id_link.items():
            item = self.crawlItem(link)
            if item is None:
                item = []
                item["error"] = "Can't crawl this item"
            item["natural_id"] = natural_id
            item["src"] = link
            items.append(item)
            print(f"{item}")
        self.close()
        return items

    def crawlItem(self, url):
        self.get_url(url)
        print(f"Visiting {url}")

        # WebDriverWait(self.driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")
        # Wait for 5 seconds
        # self.wait(5)
        # self.driver.execute_script(f'document.querySelector(".js__phone").click()')

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
        result['agent'] = {
            'avatar': seller.select_one("a").get("href"),
            'fullname': seller.select_one(".js_contact-name").get("title"),
            # 'phone': self.soup.select_one(".js__phone").get("mobile"),
            'email': email_selector.get("data-email") if email_selector else None,
        }

        result["created_at"] = datetime.now().strftime("%H:%M %d:%m:%Y")
        info = self.soup.select(".js__pr-config-item")
        result['start_date'] = info[0].select_one(".value").get_text(strip=True)
        result['end_date'] = info[1].select_one(".value").get_text(strip=True)
        return result
