import json
import pandas as pd
import os
import datetime
from io import StringIO
from parsel import Selector

class Product:
    def __init__(self, response):
        # Verificando o tipo do argumento response
        if type(response) == str:
            pass
        else:
            response = response.text
        
        date = datetime.datetime.now()
        product = Selector(response).xpath("//h1/text()").get().strip().replace(r"\\", "_").replace(r"/", "_")
        image = Selector(response).xpath("//div[@class='specs-photo-main']/a/img/@src").get()
        # Estrutura:
        self.obj = {
            'product' : product,
            'date' : f"{date.year}-{date.month}-{date.day}",
            'image' : image,
            'details' : {}
        }
        # Obter os dados da tabela:
        data = self.get_data(response)
        # Montar o JSON:
        self.create_json(data, self.obj)
    
    def get_data(self, response):
        # Verificando o tipo do argumento response
        if type(response) == str:
            pass
        else:
            response = response.text
        df = pd.read_html(StringIO(response),  attrs = {'cellspacing': '0'})
        df = pd.concat([*df], ignore_index=True)
        # Ajustando os dados:
        df.dropna(subset=[1,2], how='all', inplace=True, ignore_index=True)
        # Preenchendo os vazios na coluna 2 (índice 1)
        df[1] = df[1].ffill(axis=0)
        # Substituindo NaN por vazios
        df.fillna('', inplace=True)
        return df

    def create_json(self, data, obj):
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

    def save(self):
        # Salvando o JSON:
        name = self.obj['product']
        # Caminho para a pasta "produtos"
        folder = "products"
        # Criar a pasta "produtos" se não existir
        os.makedirs(folder, exist_ok=True)
        # Caminho completo para o arquivo
        full_path = os.path.join(folder, f"{name}.json")
        # path = os.path.join(r"products, f"{name}.json")
        with open(full_path, 'w') as file:
            json.dump(self.obj, file)
    
    def show(self):
        print(self.obj)

# Testando:
if __name__ == '__main__':
    import requests as req
    url = "https://www.gsmarena.com/huawei_matepad_se_11-13135.php"
    response = req.get(url)
    product = Product(response)
    product.save()

    print(response.status_code)