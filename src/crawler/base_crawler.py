from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time


class BaseCrawler:
    def __init__(self, driver_path):
        self.driver_path = driver_path
        self.driver = None
        self.soup = None

    def setup_driver(self, headless=False):
        chrome_options = Options()
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36")
        chrome_options.add_argument("--disable-gpu")  # Disable GPU rendering
        chrome_options.add_argument("--no-sandbox")
        if headless:
            chrome_options.add_argument("--headless")  # Run in headless mode (no browser UI)
        service = Service(self.driver_path)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def get_url(self, url):
        if self.driver is None:
            raise Exception("Driver not initialized. Call setup_driver() first.")
        self.setJwt()
        self.driver.get(url)

    def wait(self, seconds):
        time.sleep(seconds)

    def filter_script(self, page_source):
        self.soup = BeautifulSoup(page_source, "html.parser")
        for script in self.soup(["script", "style", "link", "meta", "iframe"]):
            script.decompose()

    def close(self):
        if self.driver:
            self.driver.quit()
