import pymongo
import logging
# Configurando o logging para exibir apenas erros
logging.getLogger('pymongo').setLevel(logging.CRITICAL)

# Conectando ao MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["GSMArena"]

# Retornando dados do banco, com base no campo de na collection
def get_data(field:str, collection_name:str):
    data = []
    collection = db[collection_name]
    for product in collection.find():
        data.append(product[field])
    return data

# Inserindo dados no banco
def insert_data(obj:dict, collection_name:str):
    collection = db[collection_name]
    collection.insert_one(obj)

# Limpando dados do banco
def clear_data():
    try:
        collection = db['New Products']
        # Remove todos os documentos da collection
        collection.delete_many({})
        print("The collection New Products has been successfully cleaned !")
    except Exception:
        pass