import os
from dotenv import load_dotenv
from datetime import datetime
from utils import (
    insert_bronze_layer, 
    insert_silver_layer, 
    insert_gold_layer
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
)

load_dotenv()


if __name__ == '__main__':

    creds = dict(
        endpoint_url=os.getenv('ENDPOINT'),
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )

    # TODO: Download da string do xml
    start = datetime(2024, 10, 1)
    end = datetime(2024, 10, 24)

    # insert_bronze_layer(['INCINERACAO', 'ESTORNO-INCINERACAO'], start, end, True, **creds)
    insert_silver_layer(**creds)
    insert_gold_layer('INCINERACAO', 'notas', **creds)
