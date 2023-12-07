import time

import beapder
from beapder.utils.webdriver import WebDriver


class TestRender(beapder.AirSpider):
    def start_requests(self):
        yield beapder.Request("http://www.baidu.com", render=True)

    def parse(self, request, response):
        browser: WebDriver = response.browser
        browser.find_element_by_id("kw").send_keys("beapder")
        browser.find_element_by_id("su").click()
        time.sleep(5)
        print(browser.page_source)


if __name__ == "__main__":
    TestRender().start()
