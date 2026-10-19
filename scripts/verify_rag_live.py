"""Real multilingual embedding model, PostgreSQL pgvector, and semantic retrieval gate.
Requires network once to obtain the configured model weights. No mocked vectors.
"""
from __future__ import annotations
import argparse,json,os,time
from pathlib import Path


def verify(report:Path)->dict:
    from sentence_transformers import SentenceTransformer
    import psycopg
    from rag.build_embeddings import index_records
    from rag.search import search
    dsn=os.environ['SUNSET_PG_DSN']
    model_id=os.environ.get('RAG_MODEL','sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
    started=time.monotonic()
    model=SentenceTransformer(model_id)
    inputs=[
        {'comentario_id':'REAL-CI-001','texto':'El cliente pide información sobre los pagos y las cuotas del contrato.'},
        {'comentario_id':'REAL-CI-002','texto':'La llamada recibió quejas por demora en la confirmación de reservaciones.'},
    ]
    first=index_records(inputs,model,model_id,dsn)
    second=index_records(inputs,model,model_id,dsn)
    if first['embedded']!=2 or second['embedded']!=0 or second['unchanged_skipped']!=2:
        raise AssertionError(f'Embedding upsert not idempotent: {first} {second}')
    hits=search('¿Qué dijo el cliente de los pagos?',dsn,model_id,2)
    if len(hits)!=2 or any(not (-1.00001<=h['similarity']<=1.00001) for h in hits):
        raise AssertionError('Real vector retrieval failed')
    outcome={'status':'PASS','engine':'sentence-transformers + PostgreSQL pgvector',
             'simulation':False,'model_id':model_id,'embedding_dimension':model.get_sentence_embedding_dimension(),
             'first_run':first,'replay':second,'retrieved':len(hits),
             'elapsed_seconds':round(time.monotonic()-started,3)}
    report.parent.mkdir(parents=True,exist_ok=True)
    report.write_text(json.dumps(outcome,indent=2,ensure_ascii=False)+'\n')
    return outcome

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--report',type=Path,default=Path('reports/real_rag_verified.json'))
    a=parser.parse_args()
    print(json.dumps(verify(a.report),indent=2,ensure_ascii=False))
