#!/usr/bin/env python3
"""Deterministic, memory-bounded synthetic corporate source simulator.

The exam refers to ~4.5M records in all Voltux; treating them as 4.5M
leads_ventas is an explicit STRESS-TEST assumption, not an official breakdown.
TMS counts (payments/comments) follow the brief as separate tables.
"""
from __future__ import annotations
import argparse,csv,itertools,json
from datetime import date,timedelta
from pathlib import Path

START=date(2024,1,1)
DAYS=(date(2025,12,31)-START).days+1
SALAS=[('S001','Cancún','Sala Caribe'),('S002','Riviera Maya','Sala Riviera'),('S003','Los Cabos','Sala Pacífico')]
FUENTES=[('F01','Web'),('F02','Call Center'),('F03','Referido'),('F04','Hotel')]
PRODUCTOS=[('P01','Membresía Básica'),('P02','Membresía Plus'),('P03','Membresía Premium')]
COMENTARIOS=['Atención amable durante el recorrido','Solicita información sobre cuotas',
             'Desea seguimiento de beneficios','Buena explicación del contrato',
             'Interesado en próximos viajes']

def ident(prefix:str,i:int)->str:
    return f'{prefix}{i:09d}'

def fecha(i:int)->str:
    return (START+timedelta(days=(i*13)%DAYS)).isoformat()

def cliente_for_lead(i:int,n_clientes:int)->int:
    return ((i-1)%n_clientes)+1

def write_csv(dest:Path,headers,rows,chunk_size:int=50000):
    if chunk_size<=0:raise ValueError('chunk_size must be positive')
    with dest.open('w',encoding='utf-8',newline='') as fp:
        wr=csv.writer(fp);wr.writerow(headers)
        iterator=iter(rows)
        while batch:=list(itertools.islice(iterator,chunk_size)):
            wr.writerows(batch)

def build(out:Path,leads:int,payments:int,comments:int,
          start_date:str='2024-01-01',end_date:str='2025-12-31',seed:int=0,
          chunk_size:int=50000,catalogs:dict|None=None)->dict[str,int]:
    if leads<10 or payments<1 or comments<1:raise ValueError('Need 10 leads, 1 payment/comment')
    start=date.fromisoformat(start_date);end=date.fromisoformat(end_date)
    days=(end-start).days+1
    if days<1:raise ValueError('end_date must be >= start_date')
    if chunk_size<1:raise ValueError('chunk_size must be positive')
    defaults={'salas':SALAS,'fuentes':FUENTES,'productos':PRODUCTOS,'comentarios':COMENTARIOS}
    catalog=dict(defaults);catalog.update(catalogs or {})
    salas=catalog['salas'];fuentes=catalog['fuentes'];productos=catalog['productos']
    texts=catalog['comentarios']
    if not all((salas,fuentes,productos,texts)):raise ValueError('Catalogs must not be empty')
    if len({r[0] for r in salas})!=len(salas):raise ValueError('Duplicated room identifiers')
    clients=max(3,leads//3);tours=(leads*7)//10;sales=tours//5
    if not sales:raise ValueError('At least one sale required')
    def day(i):return (start+timedelta(days=(i*13+seed%days)%days)).isoformat()
    def client(i):return ident('C',cliente_for_lead(i,clients))
    def room(i):return salas[(i-1)%len(salas)][0]
    def product(i):return productos[(i-1)%len(productos)][0]
    def source(i):return fuentes[i%len(fuentes)][0]
    def sale(i):return i<=tours and i%5==0
    out.mkdir(parents=True,exist_ok=True)
    w=lambda name,cols,rows: write_csv(out/f'{name}.csv',cols,rows,chunk_size)
    w('salas',['sala_id','destino','nombre_sala'],salas)
    w('fuentes',['fuente_id','nombre_fuente'],fuentes)
    w('productos',['producto_id','nombre_producto'],productos)
    w('clientes',['cliente_id','fecha_alta','segmento'],(
      [ident('C',i),day(i),'Recurrente' if i%4==0 else 'Nuevo'] for i in range(1,clients+1)))
    w('fx_historico',['fecha','moneda_origen','moneda_destino','tipo_cambio','updated_at'],(
      [str(start+timedelta(days=d)),cc,'MXN',
       f"{1.0 if cc=='MXN' else 17.2+((d*7+seed%19)%19)*0.12:.4f}",
       str(start+timedelta(days=d))+'T23:00:00']
      for d in range(days) for cc in ('MXN','USD')))
    w('leads_ventas',[
      'lead_id','cliente_id','fuente_id','sala_id','producto_id','fecha_creacion',
      'updated_at','tour_id','venta_id','fecha_venta','moneda','monto_venta','estado_lead'],(
      [ident('L',i),client(i),source(i),room(i),product(i),day(i),day(i)+'T13:30:00',
       ident('T',i) if i<=tours else '',ident('V',i//5) if sale(i) else '',
       day(i) if sale(i) else '', 'USD' if i%2 else 'MXN',
       str(4500+(i%12)*150) if sale(i) else '',
       'VENDIDO' if sale(i) else ('RECORRIDO' if i<=tours else 'NUEVO')]
      for i in range(1,leads+1)))
    w('tours',['tour_id','lead_id','cliente_id','sala_id','fecha_tour','status_tour'],(
      [ident('T',i),ident('L',i),client(i),room(i),day(i),'REALIZADO']
      for i in range(1,tours+1)))
    w('contratos',['contrato_id','venta_id','cliente_id','fecha_contrato','estado_contrato'],(
      [ident('K',j),ident('V',j),client(j*5),day(j*5),'ACTIVO']
      for j in range(1,sales+1)))
    w('pagos',['pago_id','contrato_id','fecha_pago','moneda','monto_pago','estado_pago'],(
      [ident('PAG',j),ident('K',((j-1)%sales)+1),day((((j-1)%sales)+1)*5),
       'USD' if ((((j-1)%sales)+1)*5)%2 else 'MXN','350.00','APLICADO']
      for j in range(1,payments+1)))
    w('comentarios',['comentario_id','tour_id','cliente_id','fecha_comentario','texto','updated_at'],(
      [ident('COM',j),ident('T',((j-1)%tours)+1),client(((j-1)%tours)+1),
       day(((j-1)%tours)+1),texts[(j-1)%len(texts)],day(((j-1)%tours)+1)+'T18:00:00']
      for j in range(1,comments+1)))
    return {'clientes':clients,'leads_ventas':leads,'tours':tours,'ventas':sales,
            'contratos':sales,'pagos':payments,'comentarios':comments,
            'fx_historico':days*2,'salas':len(salas),
            'fuentes':len(fuentes),'productos':len(productos),
            'reference_voltux_rows':leads,'reference_tms_payments':payments,
            'reference_tms_comments':comments}

if __name__=='__main__':
    from sunset.config import load
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,default=Path('data/sample'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--profile',choices=['sample','volume'],default='sample')
    a=p.parse_args();c=load(a.config);n=c['profiles'][a.profile]
    print(json.dumps(build(a.out,n['leads'],n['payments'],n['comments'],
          **c['generation'],catalogs=c.get('catalogs')),indent=2))
