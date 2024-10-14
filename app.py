import scrapy
from scrapy.crawler import CrawlerProcess
import requests as req
from scrapy.selector import Selector
from io import StringIO
import pandas as pd
import datetime
import os
import json
from time import sleep
import random

def rand_interval(num1, num2):
    return sleep(random.uniform(num1,num2))

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
    sleep(0.2)
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

class GSMSpider(scrapy.Spider):
    name = "gsmbot"
    # Configurações da spider
    custom_settings = {
        'DOWNLOAD_DELAY': 3,  # Atraso de 2 segundos entre cada requisição
        'RANDOMIZE_DOWNLOAD_DELAY' : True,
        'AUTOTHROTTLE_ENABLED': True,
        #'AUTOTHROTTLE_MAX_DELAY': 10,
        #'AUTOTHROTTLE_START_DELAY': 1,
        #'AUTOTHROTTLE_TARGET_CONCURRENCY': 50, # 500
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    def start_requests(self):
        links = []
        brands = get_brand_links()
        for brand in brands:
            get_links(brand, links)
        # Exibindo a quantidade de itens:
        print(f"Quantidade de produtos : {len(links)}")
        sleep(5)
        # fazendo as requisições:
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_link)
    
    def parse_link(self, response):
        df = pd.read_html(StringIO(response.text),  attrs = {'cellspacing': '0'})
        df = pd.concat([*df], ignore_index=True)
        # Ajustando os dados:
        df.dropna(subset=[1,2], how='all', inplace=True, ignore_index=True)
        df[1] = df[1].ffill(axis=0)
        # Substituindo NaN por vazios
        df.fillna('', inplace=True)
        # Montando o JSON:
        date = datetime.datetime.now()
        product = response.xpath("//h1/text()").get().strip().replace(r"\\", "_").replace(r"/", "_")
        obj = {
            'product' : product,
            'date' : f"{date.year}-{date.month}-{date.day}",
            'image' : response.xpath("//div[@class='specs-photo-main']/a/img/@src").get(),
            'details' : {}
        }
        yield self.save_data(df, obj)
    
    def save_data(self, data, obj):
        # Preenchendo o objeto JSON:
        for i, row in data.iterrows():
            if row[0] not in obj['details'].keys():
                category = row[0]
                obj['details'][category] = {}
                # Informações internas:
                key, value = row[1], row[2]
                # Verificar se já existe a chave:
                if key not in obj['details'][category].keys():
                    obj['details'][category][key] = value
                else:
                    new_value = [obj['details'][category][key]]
                    obj['details'][category][key] = new_value
                    obj['details'][category][key].append(value)
            else:
                key, value = row[1], row[2]
                # Verificar se já existe a chave:
                if key not in obj['details'][category].keys():
                    obj['details'][category][key] = value
                else:
                    new_value = [obj['details'][category][key]]
                    obj['details'][category][key] = new_value
                    obj['details'][category][key].append(value)
        # Salvando o JSON:
        name = obj['product']
        # Nome da pasta onde serão salvos os dados:
        folder = "products"
        # Criar a pasta "produtos" se não existir
        folder_path = os.path.join(
            os.getcwd(), folder
        )
        os.makedirs(folder_path, exist_ok=True)
        # Caminho completo do arquivo JSON:
        #path = os.path.join(r"testes\products", f"{name}.json")
        file_path = os.path.join(folder_path, f"{name}.json")
        with open(file_path, 'w') as file:
            json.dump(obj, file)

if __name__ == '__main__':
    bot = CrawlerProcess()
    bot.crawl(GSMSpider)
    bot.start()