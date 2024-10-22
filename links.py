from scrapy.selector import Selector
from time import sleep
import requests as req

def get_brand_links():
    base_url = 'https://www.gsmarena.com/'
    url = 'https://www.gsmarena.com/makers.php3'
    links = []
    sleep(0.2)
    res = Selector(text=req.get(url).text)
    for link in res.xpath("//td/a/@href").getall():
        links.append(f"{base_url}{link}")
    return links

def get_links(page, links):
    base_url = "https://www.gsmarena.com/"
    res = Selector(req.get(page))
    sleep(0.1)
    for link in res.xpath("//div[@class='makers']/ul/li/a/@href").getall():
        links.append(base_url + link)
        print(base_url + link)
    # Tentando pegar outras páginas: funcionou
    next = res.xpath("//div[@class='nav-pages']/a[@title='Next page']/@href").get()
    if next is not None:
        next_page = base_url + next
        return get_links(next_page, links)
    else:
        pass