from pathlib import Path
import json
import numpy as np
import pandas as pd

DAY = pd.Timedelta(days=1)
REGIONS = {s:r for r,ss in {'N':'AC AM AP PA RO RR TO','NE':'AL BA CE MA PB PE PI RN SE','CO':'DF GO MS MT','SE':'ES MG RJ SP','S':'PR RS SC'}.items() for s in ss.split()}
CAPITALS = dict(zip('AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO'.split(), ['rio branco','maceio','macapa','manaus','salvador','fortaleza','brasilia','vitoria','goiania','sao luis','cuiaba','campo grande','belo horizonte','belem','joao pessoa','curitiba','recife','teresina','rio de janeiro','natal','porto alegre','porto velho','boa vista','florianopolis','sao paulo','aracaju','palmas']))
def days(a,b): return (a-b).dt.total_seconds()/86400

def distance(a,b,c,d):
    a,b,c,d=[np.radians(x.astype(float)) for x in (a,b,c,d)]
    h=np.sin((c-a)/2)**2+np.cos(a)*np.cos(c)*np.sin((d-b)/2)**2
    return 12742.0176*np.arcsin(np.sqrt(np.clip(h,0,1)))

def mode(s): return s.value_counts().sort_index().idxmax()
def smooth(n,m,parent,k=10): return (n*m.fillna(0)+k*parent)/(n+k)

class Writer:
    def __init__(self,path):
        self.path=Path(path); self.path.mkdir(parents=True,exist_ok=True); self.tables={}
    def write(self,name,df,keys):
        assert not df.duplicated(keys).any(),f'{name}: duplicate primary key'
        assert not df[keys].isna().any().any(),f'{name}: null primary key'
        df=df.sort_values(keys).reset_index(drop=True)
        df.to_parquet(self.path/f'{name}.parquet',index=False,compression='zstd')
        self.tables[name]={'rows':len(df),'primary_key':keys,'columns':{c:str(v) for c,v in df.dtypes.items()}}
        print(f'{name}: {len(df):,} rows',flush=True)
        return df
