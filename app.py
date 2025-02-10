from scrapy.crawler import CrawlerProcess
from gsmscraper import GSMSpider

if __name__ == '__main__':
    # Executando o bot
    bot = CrawlerProcess()
    bot.crawl(GSMSpider)
    bot.start()