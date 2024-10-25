import os
from dotenv import load_dotenv
from datetime import datetime
from utils import (
    insert_bronze_layer, 
    insert_silver_layer, 
    insert_gold_layer,
    merge_gold_layer
)
import logging
from typing import Literal


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
)

load_dotenv()


def main_etl(
    tips: list[str],
    start: datetime,
    end: datetime,
    table_name: str = 'notas',
    lazy: bool = True,
    font: str = 'dbnfe',
    dml: Literal['create', 'merge'] = 'create'
):
    creds = dict(
        endpoint_url=os.getenv('ENDPOINT'),
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )

    prefixs = insert_bronze_layer(
        tips,
        start,
        end, 
        lazy,
        font,
        **creds
    )

    if prefixs:
        insert_silver_layer(prefixs, **creds)

    if dml.lower() == 'create':
        insert_gold_layer(table_name, **creds)
    else:
        if prefixs:
            merge_gold_layer(prefixs, 'notas', **creds)


if __name__ == '__main__':
    main_etl(
        tips=[
           'INCINERACAO',
           'ESTORNO-INCINERACAO'
        ],
        start=datetime(2024, 10, 20),
        end=datetime(2024, 10, 25),
        table_name='notas',
        lazy=False,
        font='dbnfe',
        dml='merge'
    )