import pymongo
import os
import configparser
from datetime import datetime
from bson import ObjectId, errors
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = configparser.ConfigParser()
careplan_config = configparser.ConfigParser()
current_dir = os.getcwd()

config.read(os.path.join(current_dir, 'config', 'config.ini'))
careplan_config.read(os.path.join(current_dir, 'config', 'careplanconfig.ini'))

# class Json_Data:
#     @staticmethod
#     def process_data(env, phone_number):
#         try:
#             # DB Connections
#             connection_string = config.get(env, 'connection_string')
#             database_name = config.get(env, 'database_name')

#             careplan_conn_string = careplan_config.get(env, 'connection_string')
#             care_database_name = careplan_config.get(env, 'database_name')

#             client = pymongo.MongoClient(connection_string)
#             db = client[database_name]

#             care_client = pymongo.MongoClient(careplan_conn_string)
#             care_db = care_client[care_database_name]

#             def get_latest_document(docs, date_field='createdAt'):
#                 return sorted(docs, key=lambda x: x.get(date_field, datetime.min), reverse=True)[0] if docs else None

#             output = {}

#             # Step 1: Find asserterId
#             person = db['person'].find_one({"telecom.value": phone_number})
#             if not person:
#                 return {"error": "Person not found"}
#             asserter_id = str(person["_id"])
#             output["asserterId"] = asserter_id

#             # Step 2: Get latest permission doc
#             permissions = list(db['permission'].find({"asserter.reference": asserter_id}))
#             latest_permission = get_latest_document(permissions)
#             if latest_permission:
#                 rule_data = latest_permission.get("rule", {}).get("data", {})
#                 output["organizationName"] = rule_data.get("org", {}).get("display", "")
#                 output["facilityName"] = rule_data.get("fac", {}).get("display", "")
#             # Step 3: device-association
#                 device_data = list(db["device-association"].find({"asserter.reference": asserter_id}))
#                 all_device_data = [
#                     {
#                         "deviceId": device.get("device", {}).get("reference", ""),
#                         "deviceName": device.get("device", {}).get("display", ""),
#                         "serialNumber": device.get("serialNumber", "")
#                     }
#                     for device in device_data
#                 ]
#                 output["deviceData"] = all_device_data
#             # Step 4: care-plan-group-association
#             cpga_docs = list(care_db['care-plan-group-association'].find({"participant.reference": asserter_id}))
#             output["carePlanDetails"] = []

#             for doc in cpga_docs:
#                 care_plan_group = doc.get("carePlanGroup", {})
#                 try:
#                     cp_group_id = ObjectId(care_plan_group.get("reference", ""))
#                     cp_group = care_db['care-plan-group'].find_one({"_id": cp_group_id})
#                 except (errors.InvalidId, TypeError):
#                     cp_group = None

#                 if not cp_group:
#                     continue

#                 care_plan = cp_group.get("carePlan", {})
#                 care_plan_id = care_plan.get("reference", "")

#                 # Step 4: task-status
#                 task_status_docs = list(db['task-status'].find({
#                     "asserter.reference": asserter_id,
#                     "carePlan.reference": care_plan_id
#                 }))

                
#                 # Track latest task statuses
#                 assessment_tracker = {}
#                 no_ref_docs = []

#                 for task in task_status_docs:
#                     assessment_ref = task.get("assessment", {}).get("reference")
#                     if not assessment_ref:
#                         no_ref_docs.append(task)
#                         continue

#                     current_doc = assessment_tracker.get(assessment_ref)
#                     if not current_doc or (current_doc.get("createdAt", datetime.min) < task.get("createdAt", datetime.min)):
#                         assessment_tracker[assessment_ref] = task

#                 output["carePlanDetails"].append({
#                     "carePlanGroup": care_plan_group.get("display", ""),
#                     "carePlanName": care_plan.get("display", ""),
#                     "carePlanId": care_plan_id,
#                     "taskStatus": [
#                         {
#                             "status": task.get("status", ""),
#                             "assessmentId": task.get("assessmentId", ""),
#                             "assessmentName": task.get("assessmentName", "")
#                         }
#                         for task in list(assessment_tracker.values()) + no_ref_docs
#                     ]
#                 })

#             return output

#         except Exception as e:
#             logger.exception("Error in process_data")
#             return {"error": str(e)}

#         finally:
#             try:
#                 client.close()
#                 care_client.close()
#             except:
#                 pass

class Json_Data:
    @staticmethod
    def process_data(env, phone_number):
        try:
            # DB Connections
            connection_string = config.get(env, 'connection_string')
            database_name = config.get(env, 'database_name')

            careplan_conn_string = careplan_config.get(env, 'connection_string')
            care_database_name = careplan_config.get(env, 'database_name')

            client = pymongo.MongoClient(connection_string)
            db = client[database_name]

            care_client = pymongo.MongoClient(careplan_conn_string)
            care_db = care_client[care_database_name]

            output = {}

            # Step 1: Find person
            person = db['person'].find_one(
                {"telecom.value": phone_number},
                {"_id": 1}
            )
            if not person:
                return {"error": "Person not found"}

            asserter_id = str(person["_id"])
            output["asserterId"] = asserter_id

            # Step 2: Get latest permission doc
            latest_permission = db['permission'].find_one(
                {"asserter.reference": asserter_id},
                sort=[("createdAt", pymongo.DESCENDING)],
                projection={"rule.data.org.display": 1, "rule.data.fac.display": 1}
            )

            if latest_permission:
                rule_data = latest_permission.get("rule", {}).get("data", {})
                output["organizationName"] = rule_data.get("org", {}).get("display", "")
                output["facilityName"] = rule_data.get("fac", {}).get("display", "")

                # Step 3: device-association
                device_cursor = db["device-association"].find(
                    {"asserter.reference": asserter_id},
                    {"device.reference": 1, "device.display": 1, "serialNumber": 1}
                )
                output["deviceData"] = [
                    {
                        "deviceId": device.get("device", {}).get("reference", ""),
                        "deviceName": device.get("device", {}).get("display", ""),
                        "serialNumber": device.get("serialNumber", "")
                    }
                    for device in device_cursor
                ]

            # Step 4: care-plan-group-association
            cpga_docs = list(care_db['care-plan-group-association'].find(
                {"participant.reference": asserter_id},
                {"carePlanGroup": 1}
            ))
            care_plan_group_ids = []
            cpga_map = {}

            for doc in cpga_docs:
                ref_id = doc.get("carePlanGroup", {}).get("reference")
                if ref_id:
                    try:
                        oid = ObjectId(ref_id)
                        care_plan_group_ids.append(oid)
                        cpga_map[str(oid)] = doc.get("carePlanGroup", {})
                    except Exception:
                        continue

            cp_groups = care_db['care-plan-group'].find(
                {"_id": {"$in": care_plan_group_ids}},
                {"carePlan": 1}
            )

            output["carePlanDetails"] = []

            for cp_group in cp_groups:
                group_id_str = str(cp_group["_id"])
                care_plan = cp_group.get("carePlan", {})
                care_plan_id = care_plan.get("reference", "")

                # Step 5: task-status
                task_status_docs = list(db['task-status'].find(
                    {
                        "asserter.reference": asserter_id,
                        "carePlan.reference": care_plan_id
                    },
                    {
                        "status": 1,
                        "assessmentId": 1,
                        "assessmentName": 1,
                        "assessment.reference": 1,
                        "createdAt": 1
                    }
                ))

                # Track latest assessment docs
                assessment_tracker = {}
                no_ref_docs = []

                for task in task_status_docs:
                    ref = task.get("assessment", {}).get("reference")
                    if not ref:
                        no_ref_docs.append(task)
                        continue
                    current = assessment_tracker.get(ref)
                    if not current or current.get("createdAt", datetime.min) < task.get("createdAt", datetime.min):
                        assessment_tracker[ref] = task

                output["carePlanDetails"].append({
                    "carePlanGroup": cpga_map.get(group_id_str, {}).get("display", ""),
                    "carePlanName": care_plan.get("display", ""),
                    "carePlanId": care_plan_id,
                    "taskStatus": [
                        {
                            "status": task.get("status", ""),
                            "assessmentId": task.get("assessmentId", ""),
                            "assessmentName": task.get("assessmentName", "")
                        }
                        for task in list(assessment_tracker.values()) + no_ref_docs
                    ]
                })

            return output

        except Exception as e:
            logger.exception("Error in process_data")
            return {"error": str(e)}

        finally:
            try:
                client.close()
                care_client.close()
            except Exception:
                pass