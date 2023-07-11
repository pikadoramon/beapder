import beapder


class XueQiuSpider(beapder.AirSpider):
    def start_requests(self):
        for i in range(10):
            yield beapder.Request("https://baidu.com/#{}".format(i), render=True)

    def parse(self, request, response):
        print(response.cookies.get_dict())
        print(response.headers)
        print(response.browser)
        print("response.url ", response.url)

        # article_list = response.xpath('//div[@class="detail"]')
        # for article in article_list:
        #     title = article.xpath("string(.//a)").extract_first()
        #     print(title)


if __name__ == "__main__":
    XueQiuSpider(thread_count=1).start()