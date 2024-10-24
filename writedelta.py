from deltalake import write_deltalake
import pyarrow.parquet as pq
import pyarrow.fs as fs
from schemas.notas import schema_nota


class WriteDelta:
    def __init__(
        self,
        controle: str,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ) -> None:
        
        self.controle = controle
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def file_system_s3(self):
        return fs.S3FileSystem(
            endpoint_override=self.endpoint_url,
            access_key=self.aws_access_key_id,
            secret_key=self.aws_secret_access_key,
            request_timeout=3600
        )
    
    def read_parquet(
        self, 
        bucket_name: str = 'silver'
    ):
        system_s3 = self.file_system_s3()

        dataset = pq.ParquetDataset(
            f'{bucket_name}/{self.controle}',
            schema=schema_nota,
            filesystem=system_s3
        )

        return dataset.read(use_threads=True)

    def delta_write(
        self,
        bucket_name: str = 'gold',
        table_name: str = 'notas'
    ):
        data = self.read_parquet()

        storage_options = {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_S3_USE_HTTPS": "0",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        write_deltalake(
            f's3://{bucket_name}/{table_name}',
            data,
            schema=schema_nota,
            storage_options=storage_options,
            mode="overwrite",
            partition_by=['controle', 'year', 'month']
        )