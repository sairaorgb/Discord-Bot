import pymongo 
import json 
class pyinterface:
    def __init__(self,db_name,db_url,schema_path):
        self.db_name=db_name
        self.db_url=db_url
        self.schema_path=schema_path
        self.client=pymongo.MongoClient(db_url)                      # pymongo lib to connect to mongodb
        self.db=self.client[self.db_name]
    def insert_db(self,json_path):                                   # inserting data   
        with open(self.schema_path,'r') as sch_file:                 # schema data is loaded 
            sch_data=json.load(sch_file)
        with open(json_path,'r') as json_file:                       # sample data is loaded
            json_data=json.load(json_file)
        for doc in json_data:
            for key,value in sch_data.items():                       # Each collection in db is taken for consideration 
                collection=self.db[key]
                attr_data={}
                for i in doc.keys():                                 
                    if i.lower() in value.keys():
                        attr_data.update({i.lower():doc[i]})         # attr_data contains the attributes that match current collection in use 
                ex_entry = collection.find_one(attr_data)            # if the doc is yet to be inserted , ex_entry will be None (to avoid duplicates )
                if ex_entry==None:
                    if isinstance(attr_data,list):
                        collection.insert_many(attr_data)
                    elif isinstance(attr_data,dict):
                        collection.insert_one(attr_data)
                    else:
                        print("Invalid Json file format")
    def query_select(self):
        for doc in self.db:
            print(doc)

            
if __name__=="__main__":
    db_name     = "offcampus"                                                                  # database_name 
    db_url      = "mongodb+srv://sairaorgb398:Sabari%40123@cluster0.kgww5k3.mongodb.net/"      # srv url of data base 
    schema_path = "./schema.json"
    json_path   = "./data.json"

    mongo_obj = pyinterface(db_name,db_url,schema_path)
    mongo_obj.insert_db(json_path)
