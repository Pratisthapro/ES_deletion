import time
import os
import json
import requests
from elasticsearch import Elasticsearch, exceptions
import snowflake.connector

# ------------------- Elasticsearch Configuration -------------------

ES_HOST = "https://vp7a.us-east-1.es.amazonaws.com"
ES_USERNAME = "ban"
ES_PASSWORD = "aasf"
ES_INDEX = "pd_patients_sbr_sf_v1"

# ------------------- Get Snowflake Connection from Credential Service -------------------

def get_snowflake_connection():
    try:
        # Get credential service host and port from environment variables
        dcm_host = os.environ.get("INDATA_WORKFLOW_HOST")
        dcm_port = os.environ.get("INDATA_WORKFLOW_PORT")
        datastore_name = "warehouse"

        if not dcm_host or not dcm_port:
            raise ValueError("Missing INDATA_WORKFLOW_HOST or INDATA_WORKFLOW_PORT in environment variables.")

        # Call credential service
        response = requests.get(
            f"http://{dcm_host}:{dcm_port}/collection/credential?datastore_name={datastore_name}&user_id=dap_user"
        )
        response.raise_for_status()
        credentials = response.json()["data"]

        # Extract Snowflake credentials
        user = credentials['username']
        password = credentials['password']

        # Connect to Snowflake
        con = snowflake.connector.connect(
            user=user,
            password=password,
            account='cwholloprod',
            warehouse='ORDWAREHOUSE',
            database='DAP',
            schema='L1'
        )
        return con

    except Exception as e:
        print(f"[ERROR] Failed to get Snowflake connection: {e}")
        return None

# ------------------- Fetch old_empi Values -------------------

def get_old_empi_from_snowflake():
    con = get_snowflake_connection()
    if con is None:
        return []

    try:
        query = """
           SELECT DISTINCT substring(value,1,LENGTH(value)) as value 
           FROM dap.merge_case_execution_history t,    
           TABLE(FLATTEN(input => t.old_empis)) f 
           WHERE NULLIF(t.new_empi, '') IS NOT NULL AND status='success' AND EXTRACT(YEAR FROM ingested_at)>='2025';"""
        cur = con.cursor()
        cur.execute(query)
        results = cur.fetchall()
        empi_list = [row[0] for row in results if row[0]]
        print(f"[INFO] Retrieved {len(empi_list)} old_empi values from Snowflake.")
        return empi_list
    except Exception as e:
        print(f"[ERROR] Failed to fetch EMPI list: {e}")
        return []
    finally:
        try:
            cur.close()
            con.close()
        except:
            pass

# ------------------- Delete Documents from Elasticsearch -------------------

def delete_documents_by_empi(es_client, index, empi_list):
    if not empi_list:
        print("[WARN] No EMPI values provided for deletion.")
        return

    try:
        print(f"[INFO] Deleting {len(empi_list)} EMPI records from Elasticsearch...")
        response = es_client.delete_by_query(
            index=index,
            body={
                "query": {
                    "terms": {
                        "empi.keyword": empi_list
                    }
                }
            },
            conflicts="proceed",
            refresh=True
        )
        print("[SUCCESS] Delete operation response:", json.dumps(response, indent=2))
    except exceptions.NotFoundError:
        print(f"[ERROR] Index '{index}' does not exist.")
    except exceptions.RequestError as e:
        print(f"[ERROR] Request error: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error during deletion: {e}")

# ------------------- Main Runner -------------------

class Runner:
    @staticmethod
    def runner(file_object=None):
        try:
            # Connect to Elasticsearch
            es = Elasticsearch(
                [ES_HOST],
                http_auth=(ES_USERNAME, ES_PASSWORD)
            )

            # Step 1: Fetch old_empi values
            empi_list = get_old_empi_from_snowflake()

            # Step 2: Delete matching documents
            delete_documents_by_empi(es, ES_INDEX, empi_list)

            yield "success"
        except Exception as e:
            print(f"[FATAL] Runner failed: {e}")
            yield "failure"
            raise e

# ------------------- Entry Point -------------------

if __name__ == "__main__":
    for status in Runner.runner():
        print("Runner status:", status)
