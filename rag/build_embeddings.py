"""Incremental comment/transcript embeddings from partitioned Parquet to pgvector.

Privacy filter here is illustrative, not a certified PII redaction control.
No production Mantis APIs or personal records are accessed.
"""
from __future__ import annotations
import argparse,hashlib,json,os,re
from pathlib import Path

PII_PATTERNS=(
    (re.compile(r'\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b',re.I),'[EMAIL]'),
    (re.compile(r'(?<!\d)(?:\+?52[ -]?)?\d{10}(?!\d)'),'[TELEFONO]'),
)

def chunks(text,size=380,overlap=40):
    if size<=0 or size<=overlap or overlap<0:raise ValueError('size must exceed overlap')
    if not text:return []
    result=[];start=0
    while start<len(text):
        result.append(text[start:start+size])
        if start+size>=len(text):break
        start+=size-overlap
    return result

def redact(text):
    for pattern,replacement in PII_PATTERNS:
        text=pattern.sub(replacement,text)
    return text

def chunk_identity(source_type,source_id,position,model_id):
    return hashlib.sha256(f'{source_type}:{source_id}:{position}:{model_id}'.encode()).hexdigest()

def digest(content):
    return hashlib.sha256(content.encode()).hexdigest()

def iter_parquet_comments(lake:Path,batch_size:int=1000):
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError('pyarrow is needed to read comment Parquet') from exc
    folder=lake/'comentarios'
    if not (folder/'_manifest.json').exists():raise FileNotFoundError(folder/'_manifest.json')
    for path in sorted(folder.rglob('*.parquet')):
        pf=pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=batch_size,columns=['comentario_id','texto']):
            yield from batch.to_pylist()

def index_records(records,model,model_id,dsn,chunk_size=380,overlap=40,commit_every=200):
    import psycopg
    dim=model.get_sentence_embedding_dimension()
    if dim!=384:raise ValueError('Configured pgvector schema expects 384 dimensions')
    examined=skipped=encoded=0
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            for row in records:
                source_id=row['comentario_id']
                pieces=chunks(redact(row['texto']),chunk_size,overlap)
                for position,content in enumerate(pieces):
                    examined+=1
                    cid=chunk_identity('comment',source_id,position,model_id)
                    hashval=digest(content)
                    cur.execute('SELECT content_hash FROM ai.document_chunk WHERE chunk_id=%s',(cid,))
                    found=cur.fetchone()
                    if found and found[0]==hashval:
                        skipped+=1;continue
                    vector=model.encode(content,normalize_embeddings=True).tolist()
                    cur.execute('''INSERT INTO ai.document_chunk
                        (chunk_id,source_id,source_type,content,content_hash,model_id,embedding)
                        VALUES(%s,%s,%s,%s,%s,%s,%s::vector)
                        ON CONFLICT(chunk_id) DO UPDATE SET
                        content=EXCLUDED.content,content_hash=EXCLUDED.content_hash,
                        embedding=EXCLUDED.embedding,updated_at=now()''',
                        (cid,source_id,'comment',content,hashval,model_id,str(vector)))
                    encoded+=1
                    if encoded%commit_every==0:con.commit()
                # Remove stale trailing chunks when source text becomes shorter.
                all_ids=[chunk_identity('comment',source_id,i,model_id) for i in range(len(pieces))]
                if all_ids:
                    cur.execute('''DELETE FROM ai.document_chunk
                        WHERE source_id=%s AND source_type='comment' AND model_id=%s
                        AND NOT (chunk_id=ANY(%s))''',(source_id,model_id,all_ids))
                else:
                    cur.execute('''DELETE FROM ai.document_chunk WHERE source_id=%s
                        AND source_type='comment' AND model_id=%s''',(source_id,model_id))
        con.commit()
    return {'status':'PASS','chunks_examined':examined,'unchanged_skipped':skipped,'embedded':encoded,
            'model_id':model_id,'dimension':dim}

def index_parquet(lake,model_id,dsn,chunk_size=380,overlap=40):
    from sentence_transformers import SentenceTransformer
    model=SentenceTransformer(model_id)
    return index_records(iter_parquet_comments(lake),model,model_id,dsn,chunk_size,overlap)

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--lake',type=Path,default=Path('data/parquet'))
    p.add_argument('--model',default=os.getenv('RAG_MODEL','sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'))
    p.add_argument('--chunk-size',type=int,default=int(os.getenv('RAG_CHUNK_SIZE','380')))
    p.add_argument('--overlap',type=int,default=int(os.getenv('RAG_OVERLAP','40')))
    a=p.parse_args()
    print(json.dumps(index_parquet(a.lake,a.model,os.environ['SUNSET_PG_DSN'],a.chunk_size,a.overlap),indent=2))
if __name__=='__main__':main()
