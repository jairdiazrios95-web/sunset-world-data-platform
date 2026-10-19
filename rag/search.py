"""Offline illustrative retrieval API for Mantis integration boundary."""
import argparse,os,json

def search(query,dsn,model_id,limit=5):
    if not query.strip() or not 1<=limit<=20:raise ValueError('Invalid query or limit')
    from sentence_transformers import SentenceTransformer
    import psycopg
    model=SentenceTransformer(model_id)
    embedding=str(model.encode(query,normalize_embeddings=True).tolist())
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT source_id,source_type,content,
                     1-(embedding <=> %s::vector) AS similarity
                     FROM ai.document_chunk WHERE model_id=%s
                     ORDER BY embedding <=> %s::vector LIMIT %s''',
                     (embedding,model_id,embedding,limit))
            return [{'source_id':r[0],'source_type':r[1],
                     'content':r[2],'similarity':float(r[3])} for r in cur.fetchall()]

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('query');p.add_argument('--limit',type=int,default=5)
 a=p.parse_args();m=os.environ['RAG_MODEL'];print(json.dumps(search(a.query,os.environ['SUNSET_PG_DSN'],m,a.limit),ensure_ascii=False,indent=2))
