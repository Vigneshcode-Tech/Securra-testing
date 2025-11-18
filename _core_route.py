from fastapi import FastAPI, Request, Form ,HTTPException,UploadFile, Form,File,BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from routers._core_db import dbConnectionobj, truncate_collection,role_update, check_connection,db_insert,schedulingpolicies_update
from routers._core_fun import get_dbdetails, db_paramVal, db_masterVal, create_document,process_additional_json
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder
from tempfile import NamedTemporaryFile
import shutil
import io
from typing import Literal, List,Optional
from datetime import datetime
from func import Json_Data
from fastapi.responses import FileResponse
import uvicorn
import configparser
import json
import pymongo
from bson import ObjectId
from pydantic import BaseModel
import copy
import os
app = FastAPI()
from pymongo import MongoClient, UpdateOne
import uuid
import json
import pandas as pd

current_dir = os.getcwd()
config = configparser.ConfigParser()
currect_dir = os.getcwd()


config_path = os.path.join(currect_dir, 'config', "config.ini")
config.read(config_path)

# dbconfig = os.path.join(currect_dir, 'config.ini')
config_json_path = os.path.join(current_dir, 'config', 'config-PermissionRole.json')


with open(config_json_path, 'r') as config_file:
    config_data = json.load(config_file)

# Mount static and template folders
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/microservice", response_class=HTMLResponse)
async def Valueset_page(request: Request):
    return templates.TemplateResponse("microservice.html", {"request": request})

@app.get("/riskscore", response_class=HTMLResponse)
async def Riskscore_page(request: Request):
    return templates.TemplateResponse("riskscore.html", {"request": request})


@app.get("/update-PermissionRole", response_class=HTMLResponse)
async def Patient_page(request: Request):
    return templates.TemplateResponse("update-role-permission.html", {"request": request})

@app.get("/quentionary", response_class=HTMLResponse)
async def Question_page(request: Request):
    return templates.TemplateResponse("quentionary.html", {"request": request})


@app.get("/patient-details", response_class=HTMLResponse)
def show_form(request: Request):
    return templates.TemplateResponse("patient-details.html", {
        "request": request,
        "careplans": None
    })


@app.post('/documents')
async def create_documents(
    env: Literal['development_env', 'qa_env', 'preprod_env','test_env'] = Form(...),
    user_ids: str = Form(..., description="Comma-separated user IDs"),
    dateFrom: Optional[str] = Form(None, description="Start date (yyyy-mm-dd)", example="2024-01-01"),
    dateTo: Optional[str] = Form(None, description="End date (yyyy-mm-dd)", example="2024-12-31")
    ):
    # Parse dates
    user_ids_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]
    date_from_obj = None
    date_to_obj = None
    
    try:
        if dateFrom:
            date_from_obj = datetime.strptime(dateFrom, "%Y-%m-%d")
        if dateTo:
            date_to_obj = datetime.strptime(dateTo, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use yyyy-mm-dd")

    # Connect to MongoDB
    connection_string = config.get(env, "connection_string")
    database_name = config.get(env, "database_name")
    client = pymongo.MongoClient(connection_string)
    db = client[database_name]

    result_dict = {}
    #import pdb;pdb.set_trace()
    for uid in user_ids_list:
        result_dict[uid] = []
        query = {"asserter.reference": uid}
        if date_from_obj and date_to_obj:
            query["audit.crDt"] = {"$gte": date_from_obj, "$lt": date_to_obj}

        results = db['risk-scores-logs'].find(query)
        for doc in results:
            value_str = doc.get("data", {}).get("valueString", "")
            try:
                value_json = json.loads(value_str)
                result_dict[uid].append({"valueString": value_json})
            except json.JSONDecodeError:
                result_dict[uid].append({"valueString": value_str})  # Keep raw string if not valid JSON
        
    client.close()
    return result_dict




@app.post('/updatePermission')
async def update_permission(env: Literal['development_env', 'qa_env', 'demo_env','preprodDemo_env','preprod_env','test_env', 'local'] = Form(...)):
    # Connect to MongoDB
    connection_string = config.get(env, 'connection_string')
    db_name = config.get(env, 'database_name')
    client = MongoClient(connection_string)
    db = client[db_name]
    db["permission"].update_many(
                {
                "rule.data.role.display":"Technician"
                },
                {
                '$set':{
                "rule.data.role.display":"Coordinator"
                }
                }
                )
    db["permission"].update_many(
                {
                "rule.data.role.display":"Nurse Practitioner "
                },
                {
                '$set':{
                "rule.data.role.display":"Nurse Practitioner"
                }
                }
                )
    db["permission"].update_many(
                {
                "rule.data.role.display":"Clinical Manager "
                },
                {
                '$set':{
                "rule.data.role.display":"Clinical Manager"
                }
                }
                )
    db["permission"].update_many(
                {
                "rule.data.role.display":"Patient"
                },
                {
                '$set':{
                "rule.data.role.display":"Patient"
                }
                }
                )
    # Build a role-to-permission mapping for fast lookup
    roles_data = config_data.get('permission', [])
    role_map = {
        role_block["role"]["display"]: role_block["modulePermission"]
        for role_block in roles_data
    }

    # Batch update settings
    batch_size = 500
    permission_cursor = db['permission'].find({})
    bulk_ops = []

    # Diagnostic counters
    total_documents = 0
    valid_display = 0
    matched_role = 0
    updated_count = 0

    for doc in permission_cursor:
        total_documents += 1
        display = doc.get('rule', {}).get('data', {}).get('role', {}).get('display', '')

        if display:
            valid_display += 1
            permission_data = role_map.get(display)

            if permission_data is not None:
                matched_role += 1
                # Only update if the data is actually different
                existing_permission = doc.get('rule', {}).get('data', {}).get('modulePermission')
                if existing_permission != permission_data:
                    bulk_ops.append(UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': {'rule.data.modulePermission': permission_data}}
                    ))

        # Execute batch
        if len(bulk_ops) >= batch_size:
            result = db['permission'].bulk_write(bulk_ops)
            updated_count += result.modified_count
            bulk_ops.clear()

    # Final batch if any remain
    if bulk_ops:
        result = db['permission'].bulk_write(bulk_ops)
        updated_count += result.modified_count

    client.close()

    # Return full diagnostics
    return {
        "status": "completed",
        "total_documents_scanned": total_documents,
        "documents_with_display": valid_display,
        "documents_matched_to_roles": matched_role,
        "updated_documents": updated_count
    }

@app.post('/updateRoles')
async def update_roles(env: Literal['development_env', 'qa_env','demo_env','preprodDemo_env','preprod_env','test_env', 'local'] = Form(...)):
    # Connect to MongoDB
    connection_string = config.get(env, 'connection_string')
    db_name = config.get(env, 'database_name')
    client = MongoClient(connection_string)
    db = client[db_name]
    db["roles"].update_many(
                {
                "roles.disname":"Technician"
                },
                {
                '$set':{
                "roles.name":"Coordinator",
                "roles.disname":"Coordinator"
                }
                }
                )
    db["roles"].update_many(
                {
                "roles.disname":"Nurse Practitioner "
                },
                {
                '$set':{
                "roles.disname":"Nurse Practitioner"
                }
                }
                )
    db["roles"].update_many(
                {
                "roles.disname":"Clinical Manager "
                },
                {
                '$set':{
                "roles.disname":"Clinical Manager"
                }
                }
                )
    # Build a role-to-permission mapping for fast lookup
    roles_data = config_data.get('permission', [])
    role_map = {
        role_block["role"]["display"]: role_block["modulePermission"]
        for role_block in roles_data
    }

    # Batch update settings
    batch_size = 500
    role_cursor = db['roles'].find({})
    bulk_ops = []

    # Diagnostic counters
    total_documents = 0
    valid_display = 0
    matched_role = 0
    updated_count = 0

    for doc in role_cursor:
        total_documents += 1
        display = doc.get('roles', {}).get('disname', '')
        if display:
            valid_display += 1
            permission_data = role_map.get(display)

            if permission_data is not None:
                matched_role += 1
                # Only update if the data is actually different
                existing_permission = doc.get('modulePermission')
                if existing_permission != permission_data:
                    bulk_ops.append(UpdateOne(
                        {'_id': doc['_id']},
                        {'$set': {'modulePermission': permission_data}}
                    ))
        # Execute batch
        if len(bulk_ops) >= batch_size:
            result = db['roles'].bulk_write(bulk_ops)
            updated_count += result.modified_count
            bulk_ops.clear()
    # Final batch if any remain
    if bulk_ops:
        result = db['roles'].bulk_write(bulk_ops)
        updated_count += result.modified_count
    client.close()
    return {
        "status": "completed",
        "total_documents_scanned": total_documents,
        "documents_with_display": valid_display,
        "documents_matched_to_roles": matched_role,
        "updated_documents": updated_count
    }


@app.post("/submit", response_class=HTMLResponse)
def handle_form(request: Request, environment: str = Form(...), mobile_number: str = Form(...)):
    # import pdb;pdb .set_trace()
    response_data = Json_Data.process_data(environment,mobile_number)
    #response_data = json.dumps(data, indent=2)
    if "error" in response_data:
        return templates.TemplateResponse("patient-details.html", {
            "request": request,
            "error_message": response_data["error"],
            "careplans": None
        })
    final_output = json.dumps(response_data, indent=2)
    with open("output.json", "w") as f:
        f.write(final_output)
    return templates.TemplateResponse("patient-details.html", {
        "request": request,
        "environment": environment,
        "mobile_number": mobile_number,
        "asserterId": response_data["asserterId"],
        "organizationName": response_data["organizationName"],
        "facilityName": response_data["facilityName"],
        "Device_Data":response_data["deviceData"],
        "careplans": response_data["carePlanDetails"]

})
 


@app.get("/download-careplans")
def download_careplans(background_tasks: BackgroundTasks):
    file_path = os.path.join(current_dir, "output.json")
    with open(file_path, "r") as file:
        response_data = json.load(file)
    flat_data = []
    for plan in response_data["carePlanDetails"]:
        for task in plan["taskStatus"]:
            flat_data.append({
                "Asserter ID": response_data["asserterId"],
                "Organization Name": response_data["organizationName"],
                "Facility Name": response_data["facilityName"],
                "Care Plan Group": plan["carePlanGroup"],
                "Care Plan Name": plan["carePlanName"],
                "Care Plan ID": plan["carePlanId"],
                "Task Status": task["status"],
                "Assessment ID": task["assessmentId"],
                "Assessment Name": task["assessmentName"]
            })
    # Prepare deviceData sheet
    final_device_data = []
    device_data = response_data.get("deviceData", [])
    if not device_data == []:
        for device_docs in device_data:
            final_device_data.append({
                "Device_ID":device_docs['deviceId'],
                "Device_Name":device_docs['deviceName'],
                "Serial_Number":device_docs['serialNumber']
            })

    df = pd.DataFrame(flat_data)
    df_device = pd.DataFrame(final_device_data)
    # Save to a temporary Excel file
    file_id = str(uuid.uuid4())
    filename = f"careplans_{file_id}.xlsx"
    file_path = os.path.join("temp", filename)
    os.makedirs("temp", exist_ok=True)
    #df.to_excel(file_path, index=False)

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='User_Details')
        if not df_device.empty:
            df_device.to_excel(writer, index=False, sheet_name='Device_Data')
    # Register cleanup as a background task
    def cleanup_temp_folder():
        try:
            shutil.rmtree("temp")
            os.makedirs("temp")
            print("Temp folder cleaned up.")
        except Exception as e:
            print(f"Cleanup error: {e}")

    background_tasks.add_task(cleanup_temp_folder)

    # Return the file as response
    return FileResponse(file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # response = FileResponse(
    #     path=file_path,
    #     filename="CarePlans.xlsx",
    #     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    # )

    # @response.call_on_close
    # def cleanup():
    #     shutil.rmtree("temp")
    #     os.makedirs("temp")
    # return response

  



@app.post("/route_check")
async def temp_function():
    try:
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

#ROUTE - 1 return the database connection detail based on environment
#ROUTE - 2 Database connection check  --- done
#ROUTE - 3 Execute the ddb insertion


@app.post("/db_connectiondetails")
async def db_connection(request: Request):
    try:
        request_form = await request.form()
        input_data = jsonable_encoder(request_form)
        success, db_res = get_dbdetails(input_data)
        if success:
            return {
                "status": True,
                "mongodb_uname": db_res['mongodb_uname'],
                "mongodb_Pwd": db_res['mongodb_Pwd'],
                "database_name": db_res['database_name'],
                "server_url": db_res['server_url'],
                # "DataParameter_collection": db_res['DataParameter_collection']
            }
        else:
            return {"status": False, "message": "Failed to retrieve details"}
    except Exception as e:
        return {"status": False, "message": str(e)}
    
    
@app.post("/connection_check")
async def db_connection(request: Request):
    try:
        # import pdb; pdb.set_trace()
        request_form = await request.form()
        input_data = jsonable_encoder(request_form)
        db_res, org_codes, db_client = dbConnectionobj(input_data)
        if db_res:
            return {"success": True, "message":"connection sucessfully", "org_codes": org_codes}
        else:
            return {"success": False, "message":"Cannot able to connect to Database. Please check the Database inputs"}
           

            
    except Exception as e:
        return {"success": False, "error": str(e)}
    



@app.post("/execute_action")
async def db_connection(request: Request, file: UploadFile = File(None)):
    try:
        # import pdb ; pdb . set_trace()
        # If a file is provided, process it
        temp_file_path = None
        if file:
            with NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_file:
                shutil.copyfileobj(file.file, temp_file)
                temp_file_path = temp_file.name

        # Create input_data dictionary
        request_form = await request.form()
        input_data = jsonable_encoder(request_form)
        print(input_data)

        # Update input_data to include the file path, if present
        if temp_file_path:
            input_data['file'] = {
                'filename': file.filename,
                'file': temp_file_path
            }


           

        # Action handling
        if input_data['action'] == 'roles':
            print("Updating role permissions...")
            role_update_response = role_update(input_data)  # Call the function
            print(role_update_response)
            if role_update_response['success']:
                return {"success": True, "message": "Records Inserted Successfully"}
            else:
                return {"success": False, "message": "Role update failed."}


        if input_data['action'] == 'scheduling-policies':
            print("inserting scheduling-policies...")
            scheduling_policies_response = schedulingpolicies_update(input_data)
            print(scheduling_policies_response)

            if scheduling_policies_response['success']:
                return {"success": True, "message": "Records Inserted Successfully"}
            else:
                return {"success": False, "message": scheduling_policies_response['message']}



        if input_data['action'] == 'truncate_insert':
            single_orgcode = input_data["Org_Code"]
            parse_data = json.loads(single_orgcode)
            # print("sssssssssssssssss",single_orgcode)
            # print("sssssssssssssssss",parse_data)
            for code in parse_data:
                # print("teste code",code)
                
                #trun_process = truncate_collection(input_data,code)
                # print("Updating parameter values...")
                paramval_update = db_paramVal(input_data,code)     
                masterVal_update = db_masterVal(input_data,code)

                if paramval_update[0] and masterVal_update[0]:
                    # import pdb ; pdb . set_trace()
                    trun_process = truncate_collection(input_data,code)
                    paraminsert_process = db_insert(paramval_update[2],input_data)
                    masterinsert_process = db_insert(masterVal_update[2],input_data)
                    process_additional_json(input_data, code)
                    if paraminsert_process:
                            print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE ParamVal---------")
                    else:
                        print("------ Unable to update the record ParamVal----------")

                    if masterinsert_process:
                            print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE MasterVal---------")
                    else:
                        print("------ Unable to update the record ParamVal----------")
                else:
                    return {"success": False, "message": "Please check the file. Error during truncate and insert."}
                
            return {"success": True, "message": "Records Inserted Successfully"}
                
        
        if input_data['action'] == 'update':
            db_res, org_codes, db_client = dbConnectionobj(input_data)
            if db_res:
                org_code = input_data.get('Org_Code')
                add_orgcode = input_data.get('add_orgcode')

                if org_code and (org_code == 'ALL' or org_code in org_codes):
                    if not add_orgcode:
                        return {"success": False, "message": "Organization code already exists. Please provide a new organization code."}
                    elif add_orgcode in org_codes:
                        return {"success": False, "message": "Organization code already exists. Please provide a new organization code."}
                
                paramval_update = db_paramVal(input_data,add_orgcode)     
                masterVal_update = db_masterVal(input_data,add_orgcode)
                paramupdate_process = db_insert(paramval_update[2],input_data)
                masterupdate_process = db_insert(masterVal_update[2],input_data)
                process_additional_json(input_data, add_orgcode)
                if paramupdate_process:
                        print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE ParamVal---------")
                else:
                    print("------ Unable to update the record ParamVal----------")

                if masterupdate_process:
                        print("------- RECORDS ARE SUCCESSFULLY SAVED TO DATABASE MasterVal---------")
                else:
                    print("------ Unable to update the record ParamVal----------")
            else:
                return {"success": False, "message": "Please check the file. Error during truncate and insert."}
            
            return {"success": True, "message": "Records Inserted Successfully"}

        if input_data['action'] == 'rules':
            db_res, file_name, collection = check_connection(input_data)
            print("Updating rules...")
            if db_res:
                try:
                    # import pdb ; pdb .set_trace()
                    # If file_name is a file path (string)
                    with open(file_name, 'rb') as file:
                        contents = file.read()  # Read file contents
                    #contents = await file_name.read()
                    df = pd.read_excel(io.BytesIO(contents), sheet_name="Data_values", keep_default_na=False)

                    inserted_count = 0
                    skipped_count = 0

                    for _, row in df.iterrows():
                        rule_code = row['ruleCode']
                        existing_document = collection.find_one({"ruleCode": rule_code})
                        if existing_document:
                            skipped_count += 1
                        else:
                            document = create_document(row)
                            collection.insert_one(document)
                            inserted_count += 1

                    return JSONResponse(content={
                        "message": f"Processing completed successfully! Inserted {inserted_count} records, skipped {skipped_count}."
                    })
                except Exception as e:
                    return JSONResponse(status_code=500, content={"message": f"Error processing file: {str(e)}"})
            
    except Exception as e:
        return JSONResponse(status_code=400, content={"success": False, "error": str(e)})

def insert_to_db(json_data):
    try:
        # Assuming db_insert is a function to insert JSON data into the database
        db_insert_result = db_insert(json_data)
        if db_insert_result:
            print(f"Successfully inserted data for org_code: {json_data['org_code']}")
    except Exception as e:
        print(f"Error inserting data: {e}")



#questionary code



# ======== Models ========
class FacilityInfo(BaseModel):
    reference: str
    code: str
    type: str
    display : str

class OrganizationInfo(BaseModel):
    reference: str
    code: str
    type: str
    display : str

class RoleInfo(BaseModel):
    reference: str
    display: str
    code: str

class QuestionnaireRequest(BaseModel):
    env: Literal['development_env', 'qa_env', 'test_env','demo_env', 'preprod_env','preprodDemo_env','local']
    id: List[str]
    org: OrganizationInfo
    fac: FacilityInfo
    role: RoleInfo

# ======== Endpoint ========
@app.post("/Insert_question")
def update_questionnaire(request: QuestionnaireRequest):
    #print("helllo")
    #import pdb;pdb.set_trace()
    # Load DB config
    config = configparser.ConfigParser()
    config_path = os.path.join(os.getcwd(), 'config','config.ini')
    config.read(config_path)

    try:
        connection_string = config.get(request.env, 'connection_string')
        db_name = config.get(request.env, 'database_name')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Config error: {str(e)}")

    client = MongoClient(connection_string)
    db = client[db_name]

    question_col = db["question"]
    questionnaire_col = db["questionnaire"]

    logs = []

    for qid in request.id:
        try:
            original_qs = questionnaire_col.find_one({"_id": ObjectId(qid)})
            if not original_qs:
                return HTTPException(status_code=404, detail=f"Questionnaire with ID {qid} not found.")

            # Duplicate questions and update linkId
            duplicate_name = ''
            question_items = original_qs.get("item", [])
            for i, item in enumerate(question_items):
                original_question = question_col.find_one({"_id": ObjectId(item["linkId"])})
                if not original_question:
                    raise HTTPException(status_code=404, detail=f"Question with ID {item['linkId']} not found.")

                new_question = copy.deepcopy(original_question)
                new_id = ObjectId()
                new_question["_id"] = new_id
                new_question["name"] = new_question.get('name', '')+" "+duplicate_name
                new_question["meta"]["orgCode"] = request.org.code
                new_question["meta"]["facCode"] = request.fac.code
                new_question["managingOrganization"] = request.org.dict()
                new_question["managingLocation"] = request.fac.dict()
                new_question["audit"]["crDt"] = datetime.utcnow()
                new_question["audit"]["crBy"] = request.role.dict()
                new_question["audit"]["upBy"] = request.role.dict()

                result = question_col.insert_one(new_question)
                question_items[i]["linkId"] = str(result.inserted_id)

            # Duplicate questionnaire
            new_qs = copy.deepcopy(original_qs)
            new_qs["_id"] = ObjectId()
            new_qs["item"] = question_items
            new_qs["name"] = original_qs.get('name', '')+" "+duplicate_name
            new_qs["meta"]["orgCode"] = request.org.code
            new_qs["meta"]["facCode"] = request.fac.code
            new_qs["managingOrganization"] = request.org.dict()
            new_qs["managingLocation"] = request.fac.dict()
            new_qs["audit"]["crDt"] = datetime.utcnow()
            new_qs["audit"]["crBy"] = request.role.dict()
            new_qs["audit"]["upBy"] = request.role.dict()

            final_result = questionnaire_col.insert_one(new_qs)
            logs.append(f"Please find your new QuestionSetId is: {str(final_result.inserted_id)}")

        except Exception as e:
            return HTTPException(status_code=500, detail=f"Failed to process questionnaire ID {qid}: {str(e)}")

    return {"status": "success", "message": logs}