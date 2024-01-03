from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import timedelta
import json

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

with DAG(
    dag_id="dag_mongo_test",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["practice", "mongo"],
    default_args={
        "owner": "Kay",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_callback
    }
) as dag:
    def upload_to_mongo(ti, **context):
        try:
            hook = MongoHook(conn_id="edu_mongo")
            client = hook.get_conn()
            test_db = hook.get_collection(mongo_collection="test_data")
            print(f"Connected to MongoDB - {client.server_info()}")
            test_db.insert_one(context["test_dict"])
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")

    t1 = PythonOperator(
        task_id="upload-mongodb",
        python_callable=upload_to_mongo,
        op_kwargs={
            "test_dict": {
                "test_key1": 1,
                "test_key2": "test_value"
                }
        },
    )

    t1