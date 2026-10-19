"""Publish validated local Parquet snapshots to MinIO/S3 prefix.
Atomic publication semantics require a manifest pointer; object rename isn't atomic.
"""
from __future__ import annotations
import argparse,json,os
from pathlib import Path

def publish(lake:Path,bucket:str,endpoint:str,run_id:str):
    import boto3
    from botocore.config import Config
    if not run_id or '/' in run_id or '..' in run_id:raise ValueError('Unsafe run ID')
    client=boto3.client('s3',endpoint_url=endpoint,
       aws_access_key_id=os.environ['MINIO_ROOT_USER'],
       aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
       config=Config(signature_version='s3v4'))
    buckets={item['Name'] for item in client.list_buckets()['Buckets']}
    if bucket not in buckets:client.create_bucket(Bucket=bucket)
    files=sorted(lake.rglob('*.parquet'))
    if not files:raise ValueError('No files ready for publication')
    for f in files:
        client.upload_file(str(f),bucket,f'runs/{run_id}/{f.relative_to(lake).as_posix()}')
    for manifest in lake.rglob('_manifest.json'):
        client.upload_file(str(manifest),bucket,f'runs/{run_id}/{manifest.relative_to(lake).as_posix()}')
    pointer={'run_id':run_id,'file_count':len(files),'status':'COMPLETE'}
    client.put_object(Bucket=bucket,Key='manifests/latest.json',Body=json.dumps(pointer).encode())
    return pointer

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--lake',type=Path,default=Path('data/parquet'))
 p.add_argument('--bucket',default=os.getenv('SUNSET_S3_BUCKET','sunset-lake'))
 p.add_argument('--endpoint',default=os.getenv('SUNSET_S3_ENDPOINT','http://localhost:9000'))
 p.add_argument('--run-id',required=True)
 a=p.parse_args();print(json.dumps(publish(a.lake,a.bucket,a.endpoint,a.run_id),indent=2))
