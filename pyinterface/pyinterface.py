import pymongo  
import json 
class pyinterface:
    def __init__(self,db_name,db_url,oncampus_intern_schema_path,oncampus_plac_schema_path,oncampus_exp_schema_path,offcampus_schema_path):
        self.db_name=db_name
        self.db_url=db_url
        self.on_intern_schema_path=oncampus_intern_schema_path
        self.on_plac_schema_path=oncampus_plac_schema_path
        self.on_exp_schema_path=oncampus_exp_schema_path
        self.off_schema_path=offcampus_schema_path
        self.client=pymongo.MongoClient(db_url)                 # pymongo lib to connect to mongodb
        self.db=self.client[self.db_name]

    def insert_db(self,schema_path,json_path):                  # inserting data   
        with open(schema_path,'r') as sch_file:                 # schema data is loaded 
            sch_data=json.load(sch_file)
        with open(json_path,'r') as json_file:                  # sample data is loaded
            json_data=json.load(json_file)
        for doc in json_data:
            for key,value in sch_data.items():                  # Each collection in db is taken for consideration 
                collection=self.db[key]
                attr_data={}
                if (key != "company"):
                    collection_com = self.db["company"]
                    target_company = '"'+doc["company_name"].lower()+'"'
                    cursor = collection_com.find({"company_name": target_company})
                    for com in cursor:
                        attr_data.update({'company_id':com['_id']})
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

    def insert_oncampus_intern_db(self,json_path):
        self.insert_db(self.on_intern_schema_path,json_path)
    def insert_offcampus_db(self,json_path):
        self.insert_db(self.off_schema_path,json_path)
    def insert_oncampus_exp_db(self,json_path):
        self.insert_db(self.on_exp_schema_path,json_path)
    def insert_oncampus_placement_db(self,json_path):
        self.insert_db(self.on_plac_schema_path,json_path)
    

    def query_select(self):
        for doc in self.db:
            print(doc)

            
if __name__=="__main__":
    db_name     = "offcampus"                                                                  # database_name 
    db_url      = "mongodb+srv://sairaorgb398:Sabari%40123@cluster0.kgww5k3.mongodb.net/"      # srv url of data base 

    oncampus_intern_schema_path = './schemas/oncamintern.json'
    oncampus_plac_schema_path = "./schemas/oncamplace.json"
    oncampus_exp_schema_path = "./schemas/experience.json"
    offcampus_schema_path = "./schemas/offcampus.json"

    
    oncampus_intern_json_path ="./data/dataoncampintern.json"
    oncampus_plac_json_path = "./data/dataoncamplace.json"
    oncampus_exp_json_path = "./data/dataexperience.json"
    offcampus_json_path = "./data/dataoffcamp.json"
    
    mongo_obj = pyinterface(db_name,db_url,oncampus_intern_schema_path,oncampus_plac_schema_path,oncampus_exp_schema_path,offcampus_schema_path)
    mongo_obj.insert_oncampus_intern_db(oncampus_intern_json_path)
    mongo_obj.insert_oncampus_placement_db(oncampus_plac_json_path)
    mongo_obj.insert_oncampus_exp_db(oncampus_exp_json_path)
    mongo_obj.insert_offcampus_db(offcampus_json_path)
    