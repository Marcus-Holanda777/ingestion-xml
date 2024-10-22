import boto3
import os
from write import Write
from dotenv import load_dotenv
from parsexml import (
    FileXml, 
    ParseXml
)
from connect import iter_notes
from datetime import datetime
from itertools import islice
from writedelta import WriteDelta


load_dotenv()


if __name__ == '__main__':
    # TODO: Criar os buckets camada bronze and gold
    cliente = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )

    # TODO: Download da string do xml
    start = datetime(2024, 9, 1)
    end = datetime(2024, 10, 22)
    
    gen_notes = iter_notes(
        tips=['INCINERACAO', 'ESTORNO-INCINERACAO'],
        start=start,
        end=end
    )
    
    for p, data in enumerate(gen_notes, 1):
        file = FileXml(*data)
        file_to, file_bytes = file.export_file_xml()
        cliente.put_object(Body=file_bytes, Bucket='bronze', Key=file_to)

        # CAMADA SILVER
        file_bytes.seek(0)
        xml = ParseXml(file_bytes)
        write = Write(xml.arrow(), bucket_name='silver', client=cliente)
        write.write_parquet_buffer(file_to)

        print(f'Chave: {data[1]}, {p}')

    # CAMADA GOLD
    delta =  WriteDelta(
        'INCINERACAO', 
        endpoint_url='http://localhost:9000',
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )
    delta.delta_write()
