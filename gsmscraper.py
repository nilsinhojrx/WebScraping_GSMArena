import scrapy
from io import StringIO
import pandas as pd
from time import sleep
from links import get_brand_links, get_links
import datetime
from database import insert_data, get_data, clear_data
from time import sleep

class GSMSpider(scrapy.Spider):
    name = "gsmbot"

    # Configurações da spider
    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Atraso de 2 segundos entre cada requisição
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 5,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }

    def start_requests(self):
        links = []
        brands = get_brand_links()
        # Obtendo os links de todos os produtos de todas as marcas
        for brand in brands:
            get_links(brand, links)
        # Exibindo a quantidade de itens:
        print(f"Number of Products : {len(links)}")
        sleep(2)
        # Limpando a coleção New Products:
        clear_data()
        # Filtrar os produtos já existentes no banco de dados:
        self.logger.info("Filtering existing products...")
        existing_links = get_data('url', 'Products')
        links_to_scrape = [link for link in links if link not in existing_links]
        # Informando a quantidade de produtos existentes e os que precisam ser coletados:
        self.logger.info(f"Number of Existing Products : {len(existing_links)}")
        self.logger.info(f"Number of Products to Scrape : {len(links_to_scrape)}")
        # Iniciando o processo de scraping:
        sleep(2)
        self.logger.info("Start product scraping...")
        sleep(2)
        # Fazendo as requisições:
        for link in links_to_scrape:
            # verificando se a url já existe no banco de dados:
            if link not in get_data('url', 'Products'):
                yield scrapy.Request(url=link, callback=self.parse_link)
            else:
                self.logger.info("Url has already exist in the database. Skipping...")

    def parse_link(self, response):
        assert response.status == 200, f"Request failed for {response.url}"
        # Coletando os dados do produto:
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
        # Chamano a função que preencherá o JSON e o salvará no banco de dados
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
        # Salvando o JSON no banco de dados:
        name = obj['product']
        # Verificando se o produto já existe no banco de dados:
        if name in get_data('product', 'Products'):
            pass
        else:
            # Inserindo o JSON na coleção principal do banco de dados:
            insert_data(obj, 'Products')
            # Inserindo o JSON na coleção New Products:do banco de dados:
            insert_data(obj, 'New Products')
