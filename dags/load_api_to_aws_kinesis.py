from datetime import datetime
import json
import logging
import requests
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

api_base_url = "https://jsonplaceholder.typicode.com"
kinesis_client = boto3.client("kinesis")


def _set_api_user_id(**context):
    try:
        api_user_id = int(Variable.get("api_user_id", default_var=-1))
        logger.info(f"type:: {type(api_user_id)} and api_user_id:: {api_user_id}")

        if api_user_id == -1 or api_user_id >= 10:
            new_id = 1
        else:
            new_id = api_user_id + 1

        Variable.set(key="api_user_id", value=new_id)
        logger.info(f"Latest api user id set to {new_id} successfully")
        return new_id
    except Exception as e:
        logger.error(f"ERROR WHILE SETTING UP userId param value:: {e}")
        raise


def _extract_userposts(**context):
    try:
        ti = context["ti"]
        # Pull the newly updated user id directly from the previous task's output
        new_api_user_id = ti.xcom_pull(task_ids="get_api_userId_params")
        if new_api_user_id is None:
            new_api_user_id = int(Variable.get("api_user_id", default_var=1))

        logger.info(f"Extracting posts for user_id:: {new_api_user_id}")
        response = requests.get(f"{api_base_url}/posts?userId={int(new_api_user_id)}")
        response.raise_for_status()
        user_posts = response.json()

        logger.info(f"Retrieved {len(user_posts)} posts for user {new_api_user_id}")
        return user_posts  # Automatically pushed to XCom as 'return_value'
    except Exception as e:
        logger.error(f"ERROR WHILE FETCHING USER POSTS API DATA:: {e}")
        raise


def _process_user_posts(**context):
    try:
        ti = context["ti"]
        stream_name = "user-posts-data-stream"

        # Pull posts returned by extract_userposts
        user_posts = ti.xcom_pull(task_ids="extract_userposts")
        new_api_user_id = ti.xcom_pull(task_ids="get_api_userId_params")

        if not user_posts:
            logger.info("No user posts found to process.")
            return "No posts to write."

        logger.info(f"Writing {len(user_posts)} posts to Kinesis stream `{stream_name}`")

        # Write data record by record to Kinesis stream
        for user_post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(user_post) + "\n",
                PartitionKey=str(user_post["userId"]),
                SequenceNumberForOrdering=str(user_post["id"] - 1),
            )
            logger.info(
                f"Produced record {response['SequenceNumber']} to Shard {response['ShardId']}"
            )

        return f"Total {len(user_posts)} posts with user id {new_api_user_id} written into `{stream_name}`"
    except Exception as e:
        logger.error(f"ERROR WHILE WRITING USER POSTS TO KINESIS STREAM:: {e}")
        raise


with DAG(
    dag_id="load_api_aws_kinesis",
    default_args={"owner": "Sovan"},
    tags=["api data load to s3"],
    start_date=datetime(2023, 9, 24),
    schedule="@daily",
    catchup=False,
) as dag:

    get_api_userId_params = PythonOperator(
        task_id="get_api_userId_params",
        python_callable=_set_api_user_id,
    )

    extract_userposts = PythonOperator(
        task_id="extract_userposts",
        python_callable=_extract_userposts,
    )

    write_userposts_to_stream = PythonOperator(
        task_id="write_userposts_to_stream",
        python_callable=_process_user_posts,
    )

    get_api_userId_params >> extract_userposts >> write_userposts_to_stream
