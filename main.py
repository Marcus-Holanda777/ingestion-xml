import pyodbc
from contextlib import closing
import lxml.etree as ET
import pandas as pd
from dataclasses import dataclass
from typing import Callable, Any
import re
from parsexml import ParseXml, FileXml
from collections import defaultdict
import boto3
import os
from write import Write
from dotenv import load_dotenv


load_dotenv()


# TODO: Criar os buckets camada raw and bronze
cliente = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY')
)

# ---------------------------------------------

DRIVER = (
    'Driver={{ODBC Driver 18 for Sql Server}};'
    f'Server={os.getenv('SERVER')};'
    f'Database={os.getenv('DATABASE')};'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)

# TODO: Download da string do xml
with closing(pyodbc.connect(DRIVER)) as con:
    with closing(con.cursor()) as cursor:
        cursor.execute("""""")

        while lotes := cursor.fetchmany(100):
            for data in lotes:
                # CAMADA BRONZE
                file = FileXml(*data)
                file_to, seek = file.export_file_xml()
                cliente.put_object(Body=seek, Bucket='bronze', Key=file_to)

                # CAMADA SILVER
                seek.seek(0)
                xml = ParseXml(seek)
                write = Write(xml.df(), bucket_name='silver', client=cliente)
                write.write_parquet_buffer(file_to)