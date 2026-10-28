#!/usr/bin/env python3
"""Build notebook-ready Olist marts from local CSVs, without a database."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).parent/'src'))
from common import *
import base
import history
import training
import sellers
import demand

def run(args):
    started=time.time(); cfg=SimpleNamespace(train_from=pd.Timestamp(args.train_from),train_to=pd.Timestamp(args.train_to),sales_end=pd.Timestamp(args.sales_end),category_fit_to=pd.Timestamp(args.category_fit_to),smoothing_k=args.smoothing_k,active_window_days=90,churn_window_days=60,demand_from='2017-01-02')
    assert cfg.train_from<cfg.train_to<=cfg.sales_end
    assert cfg.train_from<cfg.category_fit_to<=pd.Timestamp(cfg.demand_from)+91*DAY
    assert cfg.smoothing_k>0
    root=Path(args.output).resolve(); stage=root.parent/(root.name+'.building'); stage.mkdir(parents=True,exist_ok=True); w=Writer(stage)
    t,snapshot=base.sources(Path(args.datasets)); cfg.snapshot_ts=snapshot
    for n,df in t.items():
        keys={'orders':['order_id'],'order_items':['order_id','order_item_id'],'order_payments':['order_id','payment_sequential'],'order_reviews':['review_id','order_id'],'customers':['customer_id'],'sellers':['seller_id'],'products':['product_id'],'category_translation':['product_category_name'],'geolocation':list(df.columns)}[n]
        w.write('stg_'+n,df,keys)
    z,cities=base.geo(t,w); d,f,rv,sv,x,so=base.facts(t,z,w,cfg); cal,bfs=base.calendar(d,snapshot,w)
    engine=history.Histories(t,d,f,rv,x,so,cfg); req=history.requests(d,f,rv,x,so,cfg); h=history.build(engine,req,w)
    dd,df=training.delivery(d,f,t,z,cities,cal,h,w)
    rr,rf=training.reviews(d,f,rv,sv,x,cal,h,cfg,w)
    availability=t['order_reviews'].groupby('order_id').review_answer_timestamp.max()
    for review_stage in rr:
        rr[review_stage]['label_available_ts']=rr[review_stage].order_id.map(availability)
        rr[review_stage]=w.write('ml_review_dataset_'+review_stage,rr[review_stage],['order_id'])
    churn,seg,sf=sellers.build(engine,t,x,so,cal,cfg,w); dem,dmf=demand.build(engine,x,cal,bfs,cfg,w)
    datasets={'delivery':('ml_delivery_dataset',dd,df,['delivery_days_from_approval'],'purchase_ts'),
      'review_t0':('ml_review_dataset_t0',rr['t0'],rf['t0'],['is_bad_review'],'purchase_ts'),
      'review_ts':('ml_review_dataset_ts',rr['ts'],rf['ts'],['is_bad_review'],'survey_ts'),
      'seller_churn':('ml_seller_churn_dataset',churn,sf,['is_churned'],'snapshot_date'),
      'seller_segmentation':('ml_seller_segmentation_dataset',seg,sf,[],'snapshot_date'),
      'demand':('ml_demand_dataset',dem,dmf,['y_h1','y_h2','y_h3','y_h4'],'origin')}
    contracts={}
    for task,(name,frame,features,targets,time_col) in datasets.items():
        assert not set(features)&set(targets)
        contracts[task]={'table':name,'features':features,'targets':targets,'time_column':time_col,'categorical_features':[c for c in features if pd.api.types.is_object_dtype(frame[c]) or isinstance(frame[c].dtype,pd.StringDtype)],'label_available_column':'label_available_ts' if 'label_available_ts' in frame else None}
    from validate import validate
    report=validate(t,d,f,rv,sv,so,dd,rr,churn,seg,dem,engine,req,contracts,w)
    manifest={'format_version':1,'source':'datasets/*.csv','built_at_utc':pd.Timestamp.now(tz='UTC').isoformat(),'parameters':{k:str(v) if isinstance(v,pd.Timestamp) else v for k,v in vars(cfg).items()},'source_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(args.datasets).glob('*.csv'))},'tables':w.tables,'tasks':contracts,'validation':report,'elapsed_seconds':round(time.time()-started,1)}
    (stage/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    # Publish a completed build; preserve the preceding version until the new one passes validation.
    previous=root.parent/(root.name+'.previous')
    if previous.exists(): shutil.rmtree(previous)
    if root.exists(): root.rename(previous)
    stage.rename(root)
    if previous.exists(): shutil.rmtree(previous)
    print(f'Completed in {manifest["elapsed_seconds"]} seconds: {root}',flush=True)

if __name__=='__main__':
    repo=Path(__file__).resolve().parent.parent; p=argparse.ArgumentParser()
    p.add_argument('--datasets',type=Path,default=repo/'datasets'); p.add_argument('--output',type=Path,default=repo/'marts'/'data')
    p.add_argument('--train-from',default='2017-01-01'); p.add_argument('--train-to',default='2018-08-15'); p.add_argument('--sales-end',default='2018-08-30'); p.add_argument('--category-fit-to',default='2017-04-03'); p.add_argument('--smoothing-k',type=float,default=10)
    run(p.parse_args())
