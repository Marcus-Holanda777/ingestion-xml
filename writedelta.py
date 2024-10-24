from deltalake import (
    write_deltalake, 
    DeltaTable
)
import pyarrow.parquet as pq
import pyarrow.fs as fs
from schemas.notas import schema_nota
import os
from datetime import timedelta


class WriteDelta:
    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str
    ) -> None:
        
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def storage_options(self) -> dict:
        return {
            "AWS_ACCESS_KEY_ID": self.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.aws_secret_access_key,
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_S3_USE_HTTPS": "0",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def file_system_s3(self):
        return fs.S3FileSystem(
            endpoint_override=self.endpoint_url,
            access_key=self.aws_access_key_id,
            secret_key=self.aws_secret_access_key,
            request_timeout=7200
        )
    
    def read_parquet(
        self,
        prefixs: list[str] = [],
        bucket_name: str = 'silver'
    ):
        system_s3 = self.file_system_s3()
        
        path_or_paths = f'{bucket_name}'
        if prefixs:
            alt_ext = lambda f: os.path.splitext(f)[0] + '.parquet'

            path_or_paths = [
                f'{bucket_name}/{alt_ext(pref)}' 
                for pref in prefixs
            ]

        dataset = pq.ParquetDataset(
            path_or_paths,
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

        write_deltalake(
            f's3://{bucket_name}/{table_name}',
            data,
            schema=schema_nota,
            storage_options=self.storage_options(),
            mode="overwrite"
        )
    
    def delta_merge(
        self,
        prefixs: list[str],
        bucket_name: str = 'gold',
        table_name: str = 'notas'
    ):
        data = self.read_parquet(prefixs)
        
        dt = DeltaTable(
            f's3://{bucket_name}/{table_name}', 
            storage_options=self.storage_options()
        )

        rst = (
            dt.merge(
                data,
                predicate="s.chave = t.chave and s.item = t.item",
                source_alias="s", 
                target_alias="t"
            )
            .when_not_matched_insert_all()
            .when_matched_update_all()
            .execute()
        )
        
        interval = timedelta(hours=4) 
        dt.optimize.compact(min_commit_interval=interval)
        dt.vacuum(dry_run=False)

        return rst