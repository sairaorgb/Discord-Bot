import pymongo  
import json 
import bson
import datetime

class pyinterface:
    def __init__(self,db_name,db_url,oncampus_intern_schema_path,oncampus_plac_schema_path,oncampus_exp_schema_path,offcampus_schema_path):
        self.db_name=db_name
        self.db_url=db_url
        self.on_intern_schema_path=oncampus_intern_schema_path
        self.on_plac_schema_path=oncampus_plac_schema_path
        self.on_exp_schema_path=oncampus_exp_schema_path
        self.off_schema_path=offcampus_schema_path
        self.client=pymongo.MongoClient(db_url)                 # connecting to the database 
        self.db=self.client[self.db_name]

    def insert_db(self,schema_path,json_path):                  # function for inserting data
        with open(schema_path,'r') as sch_file:                 # schema data is loaded 
            sch_data=json.load(sch_file)
        with open(json_path,'r') as json_file:                  # json data to be inserted  is loaded
            json_data=json.load(json_file)
        for doc in json_data:                                   # Each instance of data is taken for consideration
            for key,value in sch_data.items():                  # Each collection in db is taken for consideration
                collection=self.db[key]
                attr_data={}
                if (key != "company"):                                                      # company id (foreign key) for all other collections is added 
                    collection_com = self.db["company"]
                    target_company = doc["company_name"]
                    cursor = collection_com.find({'company_name': target_company})
                    attr_data.update({"additional":{}})
                    for com in cursor:
                        attr_data.update({'company_id':com['_id']})
                
                value_keys=list(value.keys())
                for i in doc.keys():                                                        # schema-matched attributes are added 
                    if i.lower() in value_keys:
                        if(isinstance(doc[i],str)):
                            attr_data.update({i.lower():doc[i].lower()})
                        else:
                            attr_data.update({i.lower():doc[i]})
                        value_keys.remove(i.lower())
                    else:                                                                   # non schema-matched attributes are added into "additional" attribute
                        if(key != "company"):
                            attr_data["additional"].update({i.lower():doc[i].lower()})
                for i in value_keys:                                                        # missing schema attributes are added with null value
                    attr_data.update({i:''})
                ex_entry = collection.find_one(attr_data)                                   # data is checked whether the doc is present (to avoid duplicates )
                if ex_entry==None:
                    if isinstance(attr_data,list):
                        collection.insert_many(attr_data)
                    elif isinstance(attr_data,dict):
                        collection.insert_one(attr_data)
                    else:
                        print("Invalid Json file format")

    def insert_oncampus_intern_db(self,json_path):                                          # following 4 functions are used for insertion
        self.insert_db(self.on_intern_schema_path,json_path)                                
    def insert_offcampus_db(self,json_path):                                             
        self.insert_db(self.off_schema_path,json_path)
    def insert_oncampus_exp_db(self,json_path):
        self.insert_db(self.on_exp_schema_path,json_path)
    def insert_oncampus_placement_db(self,json_path):
        self.insert_db(self.on_plac_schema_path,json_path)
    

    def query_select(self,collection_name,json_string):                                     # function for selecting / querying 
        query = json.loads(json_string)
        find_attr={}
        given_attr=query
        for i in given_attr.keys():                                                         # constructing query string for find
                if i!="company_name":
                    if(isinstance(given_attr[i]),int):
                        find_attr.update({{i: {"$gte":given_attr[i]}}})
                    # elif(i.lower()=="deadline"):                                          # checking whether deadline is completed or not
                    #     deadline_date=datetime.strptime(given_attr[i],"%d-%m-%y")
                    #     iso_date=deadline_date.date().isoformat()
                    #     if(datetime.datetime.now().isoformat()<iso_date):
                    #         find_attr.update({i.lower():given_attr[i]})
                    else:
                        find_attr.update({i.lower():given_attr[i]})
        if "company_name" in  query.keys():                                                 # connecting company and other collection through foreign key
            collection_com = self.db["company"]
            company_name=given_attr["company_name"]
            cursor1 = collection_com.find_one({'company_name':company_name})
            find_attr.update({'company_id': cursor1['_id']})
        collection=self.db[collection_name]
        docs = collection.find(find_attr)
        queried_docs = {}
        collection_com=self.db["company"]
        for i in docs :
            cursor = collection_com.find_one({'_id':i["company_id"]})
            i.pop("company_id")
            i.pop("_id")
            i.update({'company_name':cursor["company_name"]})
            queried_docs.update(i)
        json_output_string = json.dumps(queried_docs)
        return json_output_string
    
    
    def select_experiences(self,json_string):                                               # different selection definitions 
        self.query_select("experiences",json_string)
    def select_oncampus_internship(self,json_string):
        self.query_select("intern_job_profile",json_string)
    def select_oncampus_placement(self,json_string):
        self.query_select("placement_job_profile",json_string)
    def select_offcampus_jobprofile(self,json_string):
        self.query_select("offcampus_job_profile",json_string)
    
        
    def flush_collection(self,collection_name):                                            # emergency flush for emptying all the collection 
        collection = self.db[collection_name]
        collection.delete_many({})

    




            
if __name__=="__main__":

    db_name     = "Discord_bot_DB"                                                                  # database_name 
    db_url      = "mongodb+srv://sairaorgb398:Sabari%40123@cluster0.kgww5k3.mongodb.net/"      # srv url of data base 

    oncampus_intern_schema_path = './schema_and_data/oncamintern.json'
    oncampus_plac_schema_path = "./schema_and_data/oncamplace.json"
    oncampus_exp_schema_path = "./schema_and_data/experience.json"
    offcampus_schema_path = "./schema_and_data/offcampus.json"

    
    oncampus_intern_json_path ="./schema_and_data/dataoncampintern.json"
    oncampus_plac_json_path = "./schema_and_data/dataoncamplace.json"
    oncampus_exp_json_path = "./schema_and_data/dataexperience.json"
    offcampus_json_path = "./schema_and_data/dataoffcamp.json"
    
    mongo_obj = pyinterface(db_name,db_url,oncampus_intern_schema_path,oncampus_plac_schema_path,oncampus_exp_schema_path,offcampus_schema_path)

    # collection names for this database  {'offcampus_job_profile','experiences','intern_job_profile','placement_job_profile'} 


    # mongo_obj.insert_oncampus_intern_db(oncampus_intern_json_path)
    # mongo_obj.insert_oncampus_placement_db(oncampus_plac_json_path)
    # mongo_obj.insert_oncampus_exp_db(oncampus_exp_json_path)
    # mongo_obj.insert_offcampus_db(offcampus_json_path)

    # mongo_obj.query_select("experiences",json_string='{"company_name": "DataTech Solutions"}') 