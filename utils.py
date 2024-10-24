from parsexml import (
    FileXml, 
    ParseXml
)
import boto3
from connect import iter_notes
from write import Write
from writedelta import WriteDelta
from botocore.exceptions import ClientError
from datetime import datetime
import logging
import io
from typing import Any


def get_client_s3(
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str
) -> Any:

  try:
     client = boto3.client(
         's3',
         endpoint_url=endpoint_url,
         aws_access_key_id=aws_access_key_id,
         aws_secret_access_key=aws_secret_access_key
     )
  except ClientError:
     raise
  else:
     return client


def insert_bronze_layer(
    tips: list[str],
    start: datetime,
    end: datetime,
    lazy: bool,
    **creds
) -> list[str]:
    
    client = get_client_s3(**creds)

    gen_notes = iter_notes(
        tips=tips,
        start=start,
        end=end
    )

    if not lazy:
        logging.info('Not lazy !')
        gen_notes = list(gen_notes)

        logging.info(f'Total: {len(gen_notes)}')
    
    list_keys = []
    for p, data in enumerate(gen_notes, 1):
        file = FileXml(*data)
        file_to, file_bytes = file.export_file_xml()
        client.put_object(
            Body=file_bytes, 
            Bucket='bronze', 
            Key=file_to
        )

        logging.info(f'Item: {p}, {data[1]}')
        list_keys.append(file_to)
    
    return list_keys


def insert_silver_layer(
    prefixs: str | list[str] = None,
    **creds
) -> None:
    
    client = get_client_s3(**creds)

    def data_insert(response):
        for object_key in response:
            # NOTE: Ler objeto xml em memoria
            key = object_key['Key'] if isinstance(object_key, dict) else object_key 
            file_obj = client.get_object(Bucket='bronze', Key=key)
            conteudo = file_obj['Body'].read()
            conteudo_bytes = io.BytesIO(conteudo)
                    
            # NOTE: Exportar xml camada silver
            controle, *__, name = key.split('/')
            file_xml = ParseXml(conteudo_bytes)
            data = (
                file_xml.df()
                .assign(
                    controle = controle.strip().lower(),
                    status = int(name.split('_')[1]),
                    year = lambda _df: _df.dh_emi.dt.year,
                    month = lambda _df: _df.dh_emi.dt.month
                )
            )
            write_silver = Write(data, 'silver', client)
            write_silver.write_parquet_buffer(key)
            logging.info(f'{key=}')
    
    if all(
       [
          isinstance(prefixs, list),
          len(prefixs) > 0,
          prefixs is not None
       ]
    ):
        data_insert(prefixs)

    else:
        kwargs = {
            'Bucket': 'bronze'
        }
    
        if prefixs is not None:
            kwargs['Prefix'] = prefixs

        while True:
            response = client.list_objects_v2(**kwargs)
            if 'Contents' in response:
                data_insert(response['Contents'])
                
                token = response.get('NextContinuationToken', None)
                logging.info(f'Is token: {token}')

                if token is None:
                    break
                kwargs['ContinuationToken'] = token

            else:
                break


def insert_gold_layer(
    table_name: str,
    **creds
) -> None:
    
    logging.info(f'Start create delta table: {table_name}')
    
    delta = WriteDelta(**creds)
    delta.delta_write(table_name=table_name)

    logging.info(f'End create delta table: {table_name}')


def merge_gold_layer(
    prefixs: list[str],
    table_name: str,
    **creds
) -> None:
    
    logging.info(f'Start MERGE delta table: {table_name}')
    
    delta = WriteDelta(**creds)
    delta.delta_merge(prefixs, table_name=table_name)

    logging.info(f'End MERGE delta table: {table_name}')