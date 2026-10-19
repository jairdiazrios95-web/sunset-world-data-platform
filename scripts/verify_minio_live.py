"""Real MinIO/S3 object-storage proof using actual PyArrow Parquet files.
Uploads manifest and partition files, downloads again and validates SHA-256.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path


def sha256_file(path:Path)->str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda:f.read(1024*1024),b''):
            h.update(part)
    return h.hexdigest()


def verify(root:Path,report:Path)->dict:
    import boto3
    import botocore
    import pyarrow.parquet as pq
    from sunset.parquet import convert
    root=root.resolve()
    from tempfile import TemporaryDirectory
    from sunset.config import load
    cfg=load(root/'config/pipeline.json')
    endpoint=os.environ['SUNSET_S3_ENDPOINT']
    bucket=os.environ['SUNSET_S3_BUCKET']
    client=boto3.client('s3', endpoint_url=endpoint,
                        aws_access_key_id=os.environ['MINIO_ROOT_USER'],
                        aws_secret_access_key=os.environ['MINIO_ROOT_PASSWORD'],
                        region_name='us-east-1')
    try:
        client.head_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as exc:
        status=exc.response.get('ResponseMetadata',{}).get('HTTPStatusCode')
        if status not in (400,404):raise
        client.create_bucket(Bucket=bucket)
    total_files=total_rows=0
    digests=[]
    with TemporaryDirectory(prefix='sunset-minio-live-') as temp:
        scratch=Path(temp)
        lake=scratch/'lake'
        for source in ('leads_ventas','fx_historico'):
            info=convert(root/'data/sample',lake,root/'config/pipeline.json',source)[source]
            folder=lake/cfg['sources'][source]['target']
            originals=sorted(folder.rglob('*.parquet'))
            for file in originals:
                key='bronze/'+cfg['sources'][source]['target']+'/'+file.relative_to(folder).as_posix()
                client.upload_file(str(file),bucket,key)
                copy=scratch/'downloaded.parquet'
                client.download_file(bucket,key,str(copy))
                if sha256_file(copy)!=sha256_file(file):
                    raise AssertionError(f'S3 roundtrip SHA-256 mismatch: {key}')
                rows=pq.read_metadata(copy).num_rows
                digests.append({'key':key,'sha256':sha256_file(file),'rows':rows})
                total_rows+=rows
                total_files+=1
            if sum(item['rows'] for item in digests if item['key'].startswith('bronze/'+cfg['sources'][source]['target']+'/'))!=info['rows']:
                raise AssertionError('MinIO reconciliation failed')
            # Writing same key twice must replace rather than duplicate objects.
            for file in originals:
                key='bronze/'+cfg['sources'][source]['target']+'/'+file.relative_to(folder).as_posix()
                client.upload_file(str(file),bucket,key)
        page=client.list_objects_v2(Bucket=bucket,Prefix='bronze/')
        actual_objects=sum(1 for _ in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix='bronze/') for obj in _.get('Contents',[]))
        if actual_objects!=total_files:
            raise AssertionError(f'Object replay duplicated files: {actual_objects} vs {total_files}')
    outcome={'status':'PASS','engine':'PyArrow + actual MinIO S3 API',
             'simulation':False,'files':total_files,'parquet_rows':total_rows,
             'replay_objects':actual_objects,'objects':digests}
    report.parent.mkdir(parents=True,exist_ok=True)
    report.write_text(json.dumps(outcome,indent=2)+'\n')
    return outcome

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',type=Path,default=Path('.'))
    parser.add_argument('--report',type=Path,default=Path('reports/live_minio_verified.json'))
    a=parser.parse_args()
    print(json.dumps(verify(a.root,a.report),indent=2))
