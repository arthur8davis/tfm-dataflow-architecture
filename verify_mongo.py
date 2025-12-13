from pymongo import MongoClient
import pprint

def verify():
    client = MongoClient('mongodb://admin:admin123@localhost:27017')
    db = client['covid-db']
    coll = db['cases']
    
    count = coll.count_documents({})
    print(f"Documentos encontrados en 'cases': {count}")
    
    if count > 0:
        print("Muestra (último documento):")
        # Mostrar el último para ver la estructura nueva
        pprint.pprint(coll.find().sort([('_id', -1)]).limit(1)[0])
    
def clean():
    client = MongoClient('mongodb://admin:admin123@localhost:27017')
    db = client['covid-db']
    coll = db['cases']
    coll.delete_many({})
    print("Colección limpiada.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'clean':
        clean()
    else:
        verify()
