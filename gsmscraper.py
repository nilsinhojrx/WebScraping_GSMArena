import scrapy
from scrapy.crawler import CrawlerProcess
from io import StringIO
import pandas as pd
import json
from time import sleep
from links import get_brand_links, get_links
import datetime
import os

class GSMSpider(scrapy.Spider):
    name = "gsmbot"
    # Configurações da spider
    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Atraso de 2 segundos entre cada requisição
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_START_DELAY': 2,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 500,
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
            'url' : response.url,
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
        # Diretório atual:
        working_dir = os.getcwd()
        # Nome da pasta onde serão salvos os dados:
        folder = f"{working_dir}\\products"
        # Criar a pasta "produtos" no local o scritp, se não existir
        os.makedirs(folder, exist_ok=True)
        # Caminho completo do arquivo JSON:
        path = os.path.join(folder, f"{name}.json")
        with open(path, 'w') as file:
            json.dump(obj, file)

if __name__ == '__main__':
    bot = CrawlerProcess()
    bot.crawl(GSMSpider)
    bot.start()
