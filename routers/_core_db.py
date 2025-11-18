import configparser, os
import pymongo

import json
from pymongo import UpdateOne
import pandas as pd
import io
import os
from fastapi import FastAPI, Request, File, UploadFile, HTTPException, Form
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from datetime import datetime

config = configparser.ConfigParser()
config.read('config/config-valueset.ini')
current_dir = os.getcwd()
# config_path = os.path.join(os.path.dirname(__file__), 'config\\config.ini')

# config = configparser.ConfigParser()
# config.read('config/config.json')
    
def dbConnectionobj(input_data):
    try:
        # import pdb ; pdb .set_trace();
        # Check if input_data matches config data
        if (input_data['username'] == config.get(input_data['env'], "mongodb_username") and
            input_data['password'] == config.get(input_data['env'], "mongodb_password") and
            input_data['server_url'] == config.get(input_data['env'], "server_url")):
            
            connection_string = f"mongodb+srv://{input_data['username']}:{input_data['password']}@{input_data['server_url']}"            
            db_client = pymongo.MongoClient(connection_string, tls=True, tlsAllowInvalidCertificates=True)
            input_db_name = input_data.get('dbname')
            config_db_name = config.get(input_data['env'], "database_name")
            

            if input_db_name == config_db_name:
                db_name = config_db_name
                collection_name = input_data.get('collection_name', '')

                mydb = db_client[db_name]
                
                # Check if the collection exists
                if collection_name not in mydb.list_collection_names():
                    return False, f"Collection '{collection_name}' does not exist in the database '{db_name}'"

                # Access the collection
                mycollection = mydb[collection_name]

                # Get unique org codes from the collection
                org_codes = mycollection.distinct("meta.orgCode")
                org_codes.append('ALL')
                #print("i am hear for the orgcode",org_codes)
                return True, org_codes, db_client
            else:
                return False, 'Database name does not match'

        else:
            return False, 'Connection error'

    except Exception as e:
        return False, str(e)



def truncate_collection(input_data,code):
    
    try:
        # import pdb; pdb.set_trace()       
        if (input_data['username'] == config.get(input_data['env'], "mongodb_username") and
            input_data['password'] == config.get(input_data['env'], "mongodb_password") and
            input_data['server_url'] == config.get(input_data['env'], "server_url")):                        
            connection_string = f"mongodb+srv://{input_data['username']}:{input_data['password']}@{input_data['server_url']}"            
            db_client = pymongo.MongoClient(connection_string, tls=True, tlsAllowInvalidCertificates=True)             
            input_db_name = input_data.get('dbname')
            config_db_name = config.get(input_data['env'], "database_name")
           
            # print("cccccccccccccccccccccccc",dbcodes)     
            if input_db_name == config_db_name:                
                db_name = config_db_name
                #collection_name = config.get(input_data['env'], "DataParameter_collection") 
                collection_name = input_data.get('collection_name', '')                               
                mydb = db_client[db_name]
                mycollection = mydb[collection_name]
                if 'Org_Code' in input_data and input_data['Org_Code']:
                    org_code = input_data["Org_Code"]
                    # print("ssssssssssssssssssssss",org_code)
                    org_codes = code
                    # print("dddddddddddddddddddddd",org_codes)
                    if not isinstance(org_codes, list):
                        org_codes = [org_codes] 
                        # print("iiiiiiiiiiiii",org_codes) 
                        # print("iiiiiiiiiiiii",type(org_codes)) 
                    
                    if 'ALL' in org_codes:                        
                        mycollection.delete_many({})
                    else:                        
                        mycollection.delete_many({
                            "meta.orgCode": {"$in": org_codes}
                        })
                                 
                db_client.close()                
                return True
            else:                
                return False, "DB name mismatch: Input DB name '{input_db_name}' does not match config DB name '{config_db_name}"
        else:            
            return False, "Authentication failed: Check credentials."
    
    except Exception as error:
        print(f"Error: {error}")
       
                           
def db_insert(raw_json , input_data):
    try:
        #import pdb; pdb.set_trace()
        #print("dddddddddddddddddddddd",raw_json)
        db_res = dbConnectionobj(input_data)
        dbcodes =config['dpcodes']['code']
        if db_res[0]:
            mongodb_dbname = input_data['dbname']
            mydb = db_res[2][mongodb_dbname]
            #mycollection = mydb[config.get(input_data['env'], "DataParameter_collection")]
            mycollection = mydb[input_data.get('collection_name', '')]
            # print("iiiiiiiiiiiiiiii",input_data['Org_Code'])
            # if input_data['Org_Code'] != '["ALL"]':
            #     filtered_json = [record for record in raw_json if record['dpCode'] not in dbcodes]
            #     mycollection.insert_many(filtered_json)
            # else:
            mycollection.insert_many(raw_json)
            return True
        else:
            return False, db_res[1]
    except Exception as error:
        return False
    

def role_update(input_data):
    try:
        # import pdb; pdb.set_trace()
        # Validate credentials
        if (input_data['username'] == config.get(input_data['env'], "mongodb_username") and
            input_data['password'] == config.get(input_data['env'], "mongodb_password") and
            input_data['server_url'] == config.get(input_data['env'], "server_url")):
            
            # Create MongoDB connection
            connection_string = f"mongodb+srv://{input_data['username']}:{input_data['password']}@{input_data['server_url']}"
            db_client = pymongo.MongoClient(connection_string, tls=True, tlsAllowInvalidCertificates=True)
            # Validate database name
            input_db_name = input_data.get('dbname')
            config_db_name = config.get(input_data['env'], "database_name")
            
            if input_db_name != config_db_name:
                return {"success": False, "message": "Invalid database name"}
            
            db_name = db_client[config_db_name]
            collection_name = input_data.get('collection_name', '')
            
            if not collection_name:
                return {"success": False, "message": "Collection name not provided"}
            
            org_code = input_data.get('add_orgcode') or input_data.get('Org_Code', "Org_Code")
            print(type(org_code))
            if isinstance(org_code, str) and org_code.startswith('[') and org_code.endswith(']'):
                try:
                    org_code = json.loads(org_code)[0]
                    print(f"orgCode: {org_code}")
                except Exception as e:
                    print("Error parsing org_code:", e)
            db_name[collection_name].update_one(
                {
                "meta.orgCode":org_code,
                #"roleGroup.code":"SCL140L210L32",
                "roles.disname":"Technician"
                },
                {
                '$set':{
                "roles.name":"Coordinator",
                "roles.disname":"Coordinator"
                }
                }
                )
            roles_collection = db_name[collection_name]

            # Load roles from config.json
            config_json_path = os.path.join(current_dir,'config', 'config-valueset.json')
            if not os.path.exists(config_json_path):
                return {"success": False, "message": f"Config file not found at {config_json_path}"}
            
            with open(config_json_path, 'r') as config_file:
                config_data = json.load(config_file)
            roles_data = config_data.get('permission', [])
            roles_data1 = config_data.get('Additional_Json',[])
            #roles_data2 = config_data.get('second_roles', [])
            
            # Function to update modulePermission
            def update_model_permission(doc_from_config, org_code):

                roles = doc_from_config.get('role', {}).get('display')
                if not roles:
                    print("No role name found in config.")
                    return

                filter_query = {
                    'meta.orgCode': org_code,
                    'roles.disname': roles  # change to 'roles.name' if needed
                }

                print("Trying to match:", filter_query)

                update_result = roles_collection.update_one(
                    filter_query,
                    {'$set': {'modulePermission': doc_from_config.get('modulePermission', [])}}
                )

                if update_result.modified_count > 0:
                    print(f"✅ Updated {update_result.modified_count} document(s) with orgCode: {org_code}")
                else:
                    print(f"⚠️ No documents updated for orgCode: {org_code}")

                if roles in ['Organization Admin']:
                    doc = db_name['permission'].find_one({
                        "rule.data.org.code": org_code,
                        "rule.data.role.display": roles
                    })
                    doc_per = doc_from_config.get('modulePermission', [])
                    exist_per = doc.get('rule', {}).get('data', {}).get('modulePermission', []) if doc else []
                    
                    if exist_per != doc_per:
                        update_per = db_name["permission"].update_one(
                            {
                                "rule.data.org.code": org_code,
                                "rule.data.role.display": roles
                            },
                            {
                                '$set': {
                                    'rule.data.modulePermission': doc_per
                                }
                            }
                        )
                        if update_per.modified_count > 0:
                            print(f"✅ Updated_pre for {roles} {update_per.modified_count} document(s) with orgCode: {org_code}")
                        else:
                            print(f"⚠️ No documents updated_pre for roles: {roles}")


            # Run for each config
            for config_doc in roles_data:
                update_model_permission(config_doc, org_code)


            # # Query for specific roles
            # first_roles = list(roles_collection.find({
            #     "meta.orgCode": org_code,
            #     "roles.name": {"$in": ["Registered Nurse"]}
            # }))
            # print("first role",first_roles)
            # second_roles = list(roles_collection.find({
            #     "meta.orgCode": org_code,
            #     "roles.name": {"$in": ["Licensed Practical Nurse"]}
            # }))
            # print("second_role",second_roles)
            # Add missing roles
            #import pdb; pdb.set_trace()
            def add_roles(roles_new_data, existing_roles, meg_org_code):
                # print(roles_new_data)
                # print(meg_org_code)
                # print("existing_roles type:", type(existing_roles))
                # print("existing_roles content:", existing_roles)

                # Decode existing_roles if it's a string
                if isinstance(existing_roles, str):
                    import json
                    try:
                        existing_roles = json.loads(existing_roles)
                    except json.JSONDecodeError as e:
                        print("Error decoding existing_roles:", e)
                        return {"success": False, "message": "Invalid data format for existing_roles"}

                print("existing_roles in db")

                # If no existing roles, proceed to add
                if not existing_roles:
                    # Find the last role to generate a new code
                    test_role = roles_collection.find_one(sort=[("_id", -1)])
                    #new_code = "RL_1"  # Default code if no roles exist

                    if test_role and "roles" in test_role and "code" in test_role["roles"]:
                        try:
                            last_code = test_role["roles"]["code"]
                            # print(last_code)
                            prefix, num_part = last_code.split("_")
                            new_code = f"{prefix}_{int(num_part) + 1}"
                            print(new_code)
                        except (ValueError, IndexError):
                            return {"success": False, "message": "The code field is not in the expected format (e.g., 'RL_35')"}

                    # Add roles
                    # roles_list = roles_new_data.get('role', [])
                    #for role in roles_new_data:
                    # print("I am hear for the code", roles_new_data)
                    role_name = roles_new_data['roles']['name']
                    print(f"Adding role: {role_name} with code: {new_code}")

                    roles_new_data['meta']['orgCode'] = meg_org_code
                    roles_new_data['roles']['code'] = new_code
                    roles_new_data['createdAt'] = datetime.strptime(roles_new_data['createdAt'], "%Y-%m-%dT%H:%M:%S.%f%z")
                    roles_new_data['updatedAt'] = datetime.strptime(roles_new_data['updatedAt'], "%Y-%m-%dT%H:%M:%S.%f%z")

                    roles_collection.update_one(
                        {"meta.orgCode": org_code, "roles.name": role_name},
                        {"$set": roles_new_data},
                        upsert=True
                    )
                    # return {"success": True, "message": "Roles added successfully"}

                # If roles exist, check for duplicates and return appropriate message
                # existing_role_names = [role['roles']['name'] for role in existing_roles]
                # for role in roles_data:
                #     if role['roles']['name'] in existing_role_names:
                #         return {
                #             "success": False,
                #             "message": f"Role {role['roles']['name']} already exists in DB"
                #         }

                # return {"success": False, "message": "Some roles already exist in the database."}
            
            # import pdb; pdb.set_trace()

            for config_doc in roles_data1:
                    roleName =config_doc['roles']['name']
                    first_roles = list(roles_collection.find({
                        "meta.orgCode": org_code,
                        "roles.name": {"$in": [roleName]}
                    }))
                    #print(config_doc)
                    add_roles(config_doc, first_roles, org_code)
            # for config_doc in roles_data2:
            #     if config_doc.get('role'):
            #         add_roles(config_doc, second_roles, org_code)
            #         print("----------------------",second_roles)
           

            db_client.close()
            return {"success": True, "message": "Roles successfully managed"}
        else:
            return {"success": False, "message": "Invalid credentials"}
    except Exception as e:
        return {"success": False, "message": f"An error occurred: {str(e)}"}

def check_connection(input_data):
    try:
        #import pdb; pdb.set_trace()       
        # Check if input_data matches config data
        if (input_data['username'] == config.get(input_data['env'], "mongodb_username") and
            input_data['password'] == config.get(input_data['env'], "mongodb_password") and
            input_data['server_url'] == config.get(input_data['env'], "server_url")):                       
            connection_string = f"mongodb+srv://{input_data['username']}:{input_data['password']}@{input_data['server_url']}"            
            db_client = pymongo.MongoClient(connection_string, tls=True, tlsAllowInvalidCertificates=True)
            input_db_name = input_data.get('dbname')
            config_db_name = config.get(input_data['env'], "database_name")
                        
            if input_db_name == config_db_name:                
                db_name = config_db_name
                collection_name = input_data.get('collection_name', '')                          
                mydb = db_client[db_name]
                mycollection = mydb[collection_name]          

            file_info = input_data.get('file', {})
            file_path = file_info.get('file', '')
        
            if not file_path:
                raise ValueError("File path is not provided.")

                        
            return True, file_path, mycollection

        else:
            return False, 'Connection error'           

    except Exception as e:
        return False, str(e)



def schedulingpolicies_update(input_data):
    try:   
        # --- Validate credentials ---
        if not (
            input_data['username'] == config.get(input_data['env'], "mongodb_username")
            and input_data['password'] == config.get(input_data['env'], "mongodb_password")
            and input_data['server_url'] == config.get(input_data['env'], "server_url")
        ):
            return {"success": False, "message": "Invalid database credentials"}

        # --- Connect to MongoDB ---
        connection_string = f"mongodb+srv://{input_data['username']}:{input_data['password']}@{input_data['server_url']}"
        db_client = pymongo.MongoClient(connection_string, tls=True, tlsAllowInvalidCertificates=True)

        input_db_name = input_data.get('dbname')
        config_db_name = config.get(input_data['env'], "database_name")
        if input_db_name != config_db_name:
            return {"success": False, "message": "Invalid database name"}

        db = db_client[config_db_name]
        collection_name = input_data.get('collection_name', 'scheduling-policies')
        collection = db[collection_name]

        # --- Load JSON template ---
        config_file_path = os.path.join(os.getcwd(), "config", "config-Schedulepolicy.json")
        if not os.path.exists(config_file_path):
            return {"success": False, "message": "Template JSON file not found."}

        with open(config_file_path, "r") as f:
            base_data = json.load(f)

        # Remove old _id to avoid duplicate key error
        base_data.pop("_id", None)

        # --- Replace orgCode ---
        new_orgcode = input_data.get("add_orgcode")
        if not new_orgcode:
            return {"success": False, "message": "Missing 'add_orgcode' in input data."}
        base_data["meta"]["orgCode"] = new_orgcode
        base_data["meta"]["facCode"] = input_data.get("Fac_Code", "")

        # --- Increment serialNo dynamically ---
        last_doc = collection.find_one(sort=[("meta.serialNo", -1)])  # Get document with highest serialNo
        if last_doc and "meta" in last_doc and "serialNo" in last_doc["meta"]:
            base_data["meta"]["serialNo"] = last_doc["meta"]["serialNo"] + 1
        else:
            base_data["meta"]["serialNo"] = 1  # Start from 1 if collection is empty


        # --- Update audit and timestamps ---
        now = datetime.utcnow().isoformat() + "Z"
        base_data["audit"]["crDt"] = now
        base_data["audit"]["upDt"] = now
        base_data["audit"]["crBy"] = base_data["audit"]["upBy"] = {
            "id": "Backend",
            "name": "Backend",
            "role": "Backend"
        }
        base_data["createdAt"] = now
        base_data["updatedAt"] = now
        base_data["audit"]["ver"] = "v1.1"

        # --- Check if orgCode already exists ---
        existing = collection.find_one({"meta.orgCode": new_orgcode})
        if existing:
            return {"success": False, "message": f"Scheduling policy already exists for {new_orgcode}"}

        # --- Insert the new document ---
        collection.insert_one(base_data)
        return {"success": True, "message": f"Scheduling policy created for {new_orgcode}"}

    except Exception as e:
        return {"success": False, "message": str(e)}

