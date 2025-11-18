import configparser,datetime, os, io
import pandas as pd
from googletrans import Translator
from deep_translator import GoogleTranslator
from routers._core_db import db_insert
import math
from tempfile import NamedTemporaryFile
from bson import ObjectId
import json
import os, configparser
config = configparser.ConfigParser()




translator = Translator()
config = configparser.ConfigParser()
config.read('config/config-valueset.ini')
# config_path = os.path.join(os.path.dirname(__file__), 'app\\config\\config.ini')
datetime_obj = datetime.datetime.now()



def read_file(file,sheet_name):
    try:
        # 
        valid_extensions = [".csv", ".xlsx"]
        file_extension = os.path.splitext(file.filename)[1]

        if file_extension.lower() in valid_extensions:
            content = file.file.read()
            # content = await file.read()
            
            if file_extension.lower() == ".xlsx":
                get_df = pd.read_excel(io.BytesIO(content), sheet_name=0, na_values=['', 'NULL'], engine='openpyxl')
            else:
                get_df = pd.read_csv(io.StringIO(content.decode("utf-8")), na_values=['', 'NULL'])  
            return True, get_df
        else:
            return False, "Unsupport format, Please upload CSV or EXCEL Files"
    except Exception as err:
        return False, "Error in reading file"


def get_dbdetails(input_data):
    try:
        dbdetails = {   
            "mongodb_uname" :config.get(input_data['env'], "mongodb_username"),
            "mongodb_Pwd" : config.get(input_data['env'], "mongodb_password"),
            "server_url"  : config.get(input_data['env'], "server_url"),
            "database_name"  : config.get(input_data['env'], "database_name"),
            "DataParameter_collection" : config.get(input_data['env'], "DataParameter_collection")
        }
        return True, dbdetails
    except Exception as error:
        return False, str(error)
 
def apply_conditions(row):
    response_settings = {}

    # Add the type if present and not empty
    response_type = str(row.get('type', '')).lower()
    #response_type = row.get('type')
    if response_type and response_type != "nan" and not (isinstance(response_type, float) and math.isnan(response_type)):
        response_settings["type"] = response_type.lower()  # Convert to lowercase

    



    # List all possible fields to add
    possible_fields = {
        'minlength': 'minLength',
        'maxlength': 'maxLength',
        'isAllowPastDate': 'isAllowPastDate',
        'isAllowFutureDate': 'isAllowFutureDate',
        'rangeFrom': 'rangeFrom',
        'rangeTo': 'rangeTo',        
        #'placeHolder': 'placeHolder',
        'rangeFromLabel': 'rangeFromLabel',
        'rangeToLabel': 'rangeToLabel',
        'stepValue': 'stepValue'
    }
     # Handle timeFormat
   

    # Add fields from the row to response_settings if not empty or null
    # for row_key, settings_key in possible_fields.items():
    #     value = row.get(row_key)
    #     # Check if value is valid (not None, not empty, and not NaN)
    #     if value not in [None, "", float('nan')] and not (isinstance(value, float) and math.isnan(value)):
    #         response_settings[settings_key] = value
    for row_key, settings_key in possible_fields.items():
        value = row.get(row_key)           
        if value not in [None, "", float('nan')] and not (isinstance(value, float) and math.isnan(value)):
             # Explicitly convert the 'isAllowPastDate' and 'isAllowFutureDate' to booleans
            if row_key in ['isAllowPastDate', 'isAllowFutureDate']:
                # Check if the value is either True or False
                if isinstance(value, (int, float)) and not math.isnan(value):
                    # Convert 1 or 0 to True or False
                    response_settings[settings_key] = bool(value)
                else:
                    response_settings[settings_key] = value
            else:
                response_settings[settings_key] = value

    # Handle timeFormat
    time_format = row.get('timeFormat')
    if time_format and time_format != "nan" and not (isinstance(time_format, float) and math.isnan(time_format)):
        response_settings["timeFormat"] = {
            "valueCoding": {
                "code": time_format,  # e.g., "12 Hours" or "24 Hours"
                "display": time_format
            }
        }        


    # Handle timeInterval
    time_interval = row.get('timeInterval')
    if time_interval and time_interval != "nan" and not (isinstance(time_interval, float) and math.isnan(time_interval)):
        response_settings["timeInterval"] = {
            "valueCoding": {
                "code": time_interval,  # e.g., "1 Minute", "5 Minutes"
                "display": time_interval
            }
        }

    default_country = row.get('defaultCountry')
    if default_country and default_country != "nan" and not (isinstance(default_country, float) and math.isnan(default_country)):
        response_settings["defaultCountry"] = {
            "valueCoding": {
                "code": default_country,  # e.g., "12 Hours" or "24 Hours"
                "display": default_country
            }
        }         

    # Move timeFormat and timeInterval to the end of the dictionary if they exist
    if "timeFormat" in response_settings:
        time_format_value = response_settings.pop("timeFormat")
        response_settings["timeFormat"] = time_format_value

    if "timeInterval" in response_settings:
        time_interval_value = response_settings.pop("timeInterval")
        response_settings["timeInterval"] = time_interval_value

    if "defaultCountry" in response_settings:
        time_format_value = response_settings.pop("defaultCountry")
        response_settings["defaultCountry"] = time_format_value    

    return response_settings


def db_paramVal(input_data,code):
    try:
        # import pdb;pdb .set_trace();
        file_info = input_data.get('file', {})
        file_path = file_info.get('file', '')
        
        if not file_path:
            raise ValueError("File path is not provided.")
        dtParam = pd.read_excel(file_path, sheet_name="1. DataParameter", engine='openpyxl')
        dtversion = pd.read_excel(file_path, sheet_name="Document_History", engine='openpyxl',dtype=str)
        # print("testtttt",dtversion)
        # dtversion["Version #"] = dtversion["Version #"].astype(str)
        last_version = dtversion["Version #"].iloc[-1]
        # print("vvvvvvvvvvvvvvvv",last_version)


        # print("vvvvvvvvvvvvvvvvvvv",last_version)

        
        # Process the data
        tmpe_dtParam = dtParam[dtParam['Is Active'] == 'A']
        # print("------------------",tmpe_dtParam)
        get_dtParam = tmpe_dtParam.sort_values("CategoryName")
        get_dtVal = get_dtParam.loc[get_dtParam['ParameterValue'] == 'No']
        dbparam_list = []
    
        # Handle languages
        get_lang = []
        lang_dict = {"English": 'en', "Arabic": 'ar', "Hindi": "hi"}
        for lang in input_data['languages'].split(','):
            get_lang.append(lang_dict.get(lang, 'en'))
            
        # Iterate through the filtered DataFrame
        for index, row in get_dtVal.iterrows():
            for language in get_lang:
                try:
                    category_name = GoogleTranslator(source='en', target=language).translate(str(row['CategoryName'])) if pd.notna(row['CategoryName']) else ''
                    parameter_name = GoogleTranslator(source='en', target=language).translate(str(row['ParameterName'])) if pd.notna(row['ParameterName']) else ''
                except Exception as error:
                    print(f"Error translating row: {row}")
                    print(error)
                    continue
                
                catCode = row['CategoryCode']
                seqno = int(catCode.split('L1')[1]) if pd.notna(catCode) else 0
                
                catName = category_name if category_name else ""
                ParName = parameter_name if parameter_name else ""
                ParCode = row['ParameterCode'] if pd.notna(row['ParameterCode']) else ""
                dpName = row["ParameterNameKEY"] if pd.notna(row["ParameterNameKEY"]) else ""
                
                # Determine org_code and fac_code
                if 'Org_Code' in input_data and input_data['Org_Code']:
                    if 'ALL' in input_data['Org_Code'] and 'add_orgcode' in input_data:
                        org_code = code
                    else:
                        org_code = code
                        #print("oooooooooooooooooooooooooooooo", org_code)
                else:
                    org_code = ""

                fac_code = input_data.get('Fac_Code', "")
                
                # Handle serial number
                serial_no = int(row["serialNo"]) if pd.notna(row["serialNo"]) else 0
                
                # Apply response settings
                response_settings = apply_conditions(row)
                #print("pppppppppppppppppp",org_code)
                # Prepare the JSON structure
                dtParam_json = {
                    "meta": {
                        "orgCode": org_code,
                        "facCode": fac_code,
                        "lang": language,
                        "serialNo": serial_no
                    },
                    "level": "L2L3",
                    "seqno": seqno,
                    "ctgName": catName,
                    "ctgDispName": catName,
                    "ctgLevel": "L1",
                    "ctgSeqno": seqno,
                    "ctgCode": catCode,
                    "ctgNameKey": row['CategoryName'],
                    "dpName": ParName,
                    "dpDispName": ParName,
                    "dpLevel": "L2",
                    "dpSeqno": seqno,
                    "dpCode": ParCode,
                    "dpNameKey": dpName,
                    "dpType": row['ResponseType'],
                    "dpDispType":row['ResponseDispType'],
                    "responseSettings": response_settings,                    
                    "valueSet": "",
                    "audit": {
                        "vId": last_version,
                        "crAt": datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                        "crBy": {
                            "id": "001",
                            "name": "SecurraDM",
                            "role": "Datamodeler"
                        },
                        "updAt": datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                        "updBy": {
                            "id": "002",
                            "name": "SecurraDM",
                            "role": "Datamodeler"
                        },
                        "srcId": "Internal",
                        "src": "Internal",
                        "isActive": True
                    }
                }
                # import pdb ; pdb . set_trace();
                dbparam_list.append(dtParam_json)
            
        # Insert the processed data into the database
        #import pdb; pdb.set_trace()
        #print ("dddddddddddddddddd",dbparam_list)
        # Specify the file name
        # file_path = "dbparam_list.json"

        # # Write to a JSON file
        # with open(file_path, "w") as json_file:
        #     json.dump(dbparam_list, json_file, indent=4)
        # db_response = db_insert(dbparam_list, input_data)
        # if db_response:
        #     print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE ---------")
        # else:
        #     print("------ Unable to update the record ----------")

        return True, "Success", dbparam_list

    except Exception as error:
        return False, str(error)
    


def db_masterVal(input_data,code):
    try:
        # import pdb ; pdb . set_trace();  
        
        file_info = input_data.get('file', {})
        file_path = file_info.get('file', '')
        
        if not file_path:
            raise ValueError("File path is not provided.")
        dtMaster = pd.read_excel(file_path, sheet_name="2. dataparameter-Master-Value", engine='openpyxl', keep_default_na = False)
        dtversion = pd.read_excel(file_path, sheet_name="Document_History", engine='openpyxl',dtype=str)
        # print("testtttt",dtversion)
        # dtversion["Version #"] = dtversion["Version #"].astype(str)
        last_version = dtversion["Version #"].iloc[-1]
        
        # Filter only active records
        if 'Is Active' in dtMaster.columns:
            dtMaster = dtMaster[dtMaster['Is Active'] == 'A']
        
        # Ensure required columns are present
        required_columns = ['Categoryname', 'ParameterName', 'CategoryCode', 'ParameterCode', 'valuenameKEY', 'valuename', 'Dispvaluename','#SNO', 'valuecode','serialNo', 'ResponseType','dispOrder','taskType','ResponseDispType']
        dtMaster = dtMaster[[col for col in required_columns if col in dtMaster.columns]]

        # # Drop rows with any NaN or empty values in the required columns
        # dtMaster1 = dtMaster.dropna(subset=required_columns)

        # print("----------------",dtMaster1)

        grouped_data = dtMaster.groupby('ParameterCode')
        grouped_data_response = dtMaster.groupby('ResponseType')
        
        get_lang = []
        dbmaster_list = []
        lang_dict = {"English": 'en', "Arabic": 'ar', "Hindi": "hi"}
        for lang in input_data['languages'].split(','):
            get_lang.append(lang_dict[lang])
        
        for parameter_name, group_dt in grouped_data:
            for language in get_lang:
                lang = language
                try:
                    catg_name = GoogleTranslator(source='en', target=lang).translate(str(group_dt.iloc[0]['Categoryname']))
                    param_name = GoogleTranslator(source='en', target=lang).translate(str(group_dt.iloc[0]['ParameterName']))
                except Exception as error:
                    # print(group_dt)
                    print(error)
                    
                catCode = group_dt.iloc[0]['CategoryCode']
                
                split_result = catCode.split('L1')
                if len(split_result) > 1:
                    seqno = int(split_result[1])
                else:
                    # Handle the case where 'L1' is not in catCode or there’s an unexpected format
                    print("Error: 'L1' not found in catCode or incorrect format")
                    seqno = None
                #seqno = int(catCode.split('L1')[1])
                catName = catg_name
                ParName = param_name
                ParCode = group_dt.iloc[0]['ParameterCode']
                dpType = group_dt.iloc[0]['ResponseType']
                tmpName = group_dt.iloc[0]["valuenameKEY"].split('.')
                dpName = '.'.join(tmpName[:2])
                
                if 'Org_Code' in input_data and input_data['Org_Code']:
                    if 'ALL' in input_data['Org_Code'] and 'add_orgcode' in input_data:
                        org_code = code
                    else:
                        org_code = code
                else:
                    org_code = None
                
                fac_code = input_data.get('Fac_Code', None)
                serial_no = int(group_dt.iloc[0]["serialNo"])
                
                tmp_arr = []
                
                for index, row in group_dt.iterrows():
                    tmp_dict = dict()
                    
                    try:
                        tmp_name = GoogleTranslator(source='en', target=lang).translate(str(row['valuename']))
                    except Exception as error:
                        print(row)
                        print(error)
                    
                    tmp_dict['name'] = tmp_name
                    tmp_dict['dispName'] = row['Dispvaluename']
                    tmp_dict['code'] = row['valuecode']
                    tmp_dict['nameKey'] = row['valuenameKEY']
                    tmp_dict['dispOrder'] = row['dispOrder']
                    tmp_dict['taskType'] = row['taskType']
                    tmp_arr.append(tmp_dict)
                    
                    # tmp_arr.append(tmp_dict)
                
                #datetime_obj = datetime.now()
                # print (tmp_arr)
                
                put_json = {
                    "meta": {
                        "orgCode": org_code,
                        "facCode": fac_code,
                        "lang": lang,
                        "serialNo": serial_no,
                    },
                    "level": "L3",
                    "seqno": seqno,
                    "ctgName": catName,
                    "ctgDispName": catName,
                    "ctgLevel": "L1",
                    "ctgSeqno": seqno,
                    "ctgCode": catCode,
                    "ctgNameKey": group_dt.iloc[0]['Categoryname'],
                    "dpName": ParName,
                    "dpDispName": ParName,
                    "dpLevel": "L2",
                    "dpSeqno": seqno,
                    "dpCode": ParCode,
                    "dpNameKey": dpName, 
                    "dpType":dpType, 
                    "dpDispType":row['ResponseDispType'],             
                    "valueSet": tmp_arr,
                    "audit": {
                        "vId":last_version,
                        "crAt": datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                        "crBy": {
                            "id": "001",
                            "name": "SecurraDM",
                            "role": "Datamodeler"
                        },
                        "updAt": datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
                        "updBy": {
                            "id": "002",
                            "name": "SecurraDM",
                            "role": "Datamodeler"
                        },
                        "srcId": "Internal",
                        "src": "Internal",
                        "isActive": True
                    }
                }
                dbmaster_list.append(put_json)
        # print ("----------------------------", dbmaster_list)
        #      
       
        # db_response = db_insert(dbmaster_list, input_data)
        # if db_response:
        #     print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE ---------")
        # else:
        #     print("------ Unable to update the record ----------")
        return True, "Success",dbmaster_list
    except Exception as error:
        print("Error:", error)
        return False, "Error in Master Sheet updation"

# Function to handle missing values in the document
def handle_missing_value(value):
    if value == "NA":  
        return "NA"
    if value == "":  
        return ""
    return value
def format_ruleDp(value):
    if value == "":
        return ""
    return value.split(",")
# Function to create MongoDB document
def create_document(row):
    serial_no = row['#No']
    return {
        "_id": str(ObjectId()),
        "meta": {
            "orgCode": handle_missing_value(row.get('orgCode', "")),
            "facCode": handle_missing_value(row.get('facCode', "")),
            "lang": "en",
            "serialNo": serial_no
        },
        "ruleCode": handle_missing_value(row['ruleCode']),
        "ruleType": handle_missing_value(row['ruleType']),
        "ruleName": handle_missing_value(row['ruleName']),
        "dispName": handle_missing_value(row['dispName']),
        "ruleDescr": handle_missing_value(row['ruleDescr']),
        "ruleFile": handle_missing_value(row['ruleFile']),
        "reference": handle_missing_value(row['Reference']),
        "ruleCheck": handle_missing_value(row['ruleCheck']),
        "ruleOutcome": handle_missing_value(row['ruleOutcome']),
        "ruleCategory": handle_missing_value(row['ruleCategory']),
        "ruleModule": handle_missing_value(row['ruleModule']),
        "execution": handle_missing_value(row['execution']),
        "ruleDp": format_ruleDp(row['ruleDp']),
        "audit": {
            "ver": "1.0",
            "crDt": datetime_obj,
            "crBy": {
                "id": "6630ef827fed4f56eb10de84",
                "name": "Data Team",
                "role": "Data Engineer"
            },
            "upDt": datetime_obj,
            "upBy": {
                "id": "6630ef827fed4f56eb10de84",
                "name": "Data Team",
                "role": "Data Engineer"
            },
            "srcId": f"{row['ruleCategory']}_{row['ruleCode']}",
            "srcApp": 'Platform_rules_JSON.xlsx',
            "isActive": True
        }
    }

# current_dir = os.path()
current_dir = os.getcwd()
# config_path_vitaldata = os.path.join(os.path.dirname(__file__), 'config','vital_jsondata')

config_path_vitaldata = os.path.join(current_dir, "config", "vital_jsondata")
# print("hhhhhhhhhhh",config_path_vitaldata)

def process_additional_json(input_data, current_code):
    # import pdb; pdb.set_trace()
    # json_folder_path = r"C:\Users\securra\Videos\securra-valueset-v3\config\vital_jsondata"
    # Iterate through all JSON files in the folder
    file_info = input_data.get('file', {})
    file_path = file_info.get('file', '')
    dtversion = pd.read_excel(file_path, sheet_name="Document_History", engine='openpyxl',dtype=str)
    # dtversion["Version #"] = dtversion["Version #"]
    last_version = dtversion["Version #"].iloc[-1]
    final_vitaldata = []
    for json_file in os.listdir(config_path_vitaldata):
        # print(json_file)
        
        if json_file.endswith(".json"):
            json_path = os.path.join(config_path_vitaldata, json_file)


            try:
                # import pdb ; pdb .set_trace()
                # Load JSON content
                with open(json_path, 'r') as file:
                    json_data = json.load(file)

                if 'meta' in json_data and 'orgCode' in json_data['meta']:
                   json_data['meta']['orgCode'] = current_code

                   json_data['audit']['vId'] = last_version
                   

                   final_vitaldata.append(json_data)
                

                # Insert updated JSON into the database
                # insert_to_db(json_data)

            except Exception as e:
                print(f"Error processing {json_file}: {e}")
    db_response = db_insert(final_vitaldata, input_data)
    if db_response:
        print("Vitals inserted Successfully")
    else:
        print("Vital Data Not Inserted Plese check the Data")