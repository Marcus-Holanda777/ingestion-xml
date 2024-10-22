import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import os
import boto3


class Write:
    def __init__(
        self, 
        data: pd.DataFrame | pa.Table,
        bucket_name: str,
        client: boto3.client
    ) -> None:
        
        self.data = data
        self.bucket_name = bucket_name
        self.client = client

    def write_parquet_buffer(
        self,
        key: str,
        compression: str= 'snappy'
    ) -> None:
        
        if isinstance(self.data, pd.DataFrame):
           self.data = pa.Table.from_pandas(self.data)
        
        buffer = pa.BufferOutputStream()
        pq.write_table(self.data, buffer, compression=compression)

        out_key = os.path.splitext(key)[0] + '.parquet'

        self.client.put_object(
            Body=buffer.getvalue().to_pybytes(),
            Bucket=self.bucket_name,
            Key=out_key
        )