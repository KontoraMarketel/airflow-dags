import json
import logging
import uuid
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.sdk import Variable, task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def producer_func(data):
    logging.info(f"Kafka producer called with data: {data}")

    return [
        (data["task_id"].encode("utf-8"), json.dumps(data).encode("utf-8"))
    ]


@dag(
    "wb_ingest",
    default_args=default_args,
    description="Запуск инжеста данных из вб",
    schedule="0 1 * * *",
    catchup=False,
    tags=["wildberries", "elt", "ingest"],
)
def wb_commision_ingest():
    logging.basicConfig(level=logging.INFO)

    @task
    def generate_producer_data(**context):
        ts = context['ts']
        task_id = str(uuid.uuid4())
        wb_token = Variable.get("WB_API_TOKEN")

        return {
            "task_id": task_id,
            "wb_token": wb_token,
            "ts": ts,
        }

    payload = generate_producer_data()

    commissions = ProduceToTopicOperator(
        task_id="ingest-commissions",
        topic="ingest-commissions-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"data": payload},

    )
    fbw_incomes = ProduceToTopicOperator(
        task_id="ingest-fbw-incomes",
        topic="ingest-fbw-incomes-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"data": payload},

    )

    ads_ids = ProduceToTopicOperator(
        task_id="ingest-ads-ids",
        topic="ingest-all-ads-ids-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"data": payload},

    )

    prices = ProduceToTopicOperator(
        task_id="ingest-prices",
        topic="ingest-prices-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"data": payload},

    )

    cards = ProduceToTopicOperator(
        task_id="ingest-cards",
        topic="ingest-cards-tasks",
        producer_function=producer_func,
        kafka_config_id="kafka_default",
        producer_function_kwargs={"data": payload},

    )

    [
        commissions,
        fbw_incomes,
        ads_ids,
        prices,
        cards,
    ]


wb_commision_ingest()
