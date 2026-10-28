from dataclasses import dataclass
from pathlib import Path
from typing import Callable
import json
import pandas as pd

@dataclass
class TaskData:
    frame: pd.DataFrame
    contract: dict
    @property
    def X(self): return self.frame[self.contract['features']].copy()
    @property
    def y(self):
        columns=self.contract['targets']
        if not columns: return None
        return self.frame[columns[0]].copy() if len(columns)==1 else self.frame[columns].copy()
    def split(self,train_end,valid_end):
        """Temporal split, purging training labels unavailable at the next boundary.

        Fit preprocessing and models only on the returned train partition.
        The test partition ends at the built dataset's observation cutoff.
        """
        t=self.frame[self.contract['time_column']]; a=pd.Timestamp(train_end); b=pd.Timestamp(valid_end)
        if a>=b: raise ValueError('train_end must precede valid_end')
        masks={'train':t.lt(a),'valid':t.ge(a)&t.lt(b),'test':t.ge(b)}
        available=self.contract.get('label_available_column')
        if available:
            masks['train'] &= self.frame[available].lt(a)
            masks['valid'] &= self.frame[available].lt(b)
        return {name:TaskData(self.frame.loc[mask].copy(),self.contract) for name,mask in masks.items()}

class MartStore:
    def __init__(self,path=None,read_table:Callable[[str],pd.DataFrame]|None=None,manifest_path=None):
        self.path=Path(path or Path(__file__).parent/'data')
        self.manifest=json.loads(Path(manifest_path or self.path/'manifest.json').read_text())
        self._read=read_table or (lambda name:pd.read_parquet(self.path/f'{name}.parquet'))
    def read(self,name):
        if name not in self.manifest['tables']: raise KeyError(f'Unknown mart: {name}')
        frame=self._read(name)
        # Preserve source-independent timestamps, including after switching to SQL.
        for c,dtype in self.manifest['tables'][name]['columns'].items():
            if dtype.startswith('datetime'): frame[c]=pd.to_datetime(frame[c])
        return frame
    def task(self,name):
        contract=self.manifest['tasks'][name]; frame=self.read(contract['table'])
        required=contract['features']+contract['targets']+[contract['time_column']]
        missing=set(required)-set(frame)
        if missing: raise ValueError(f'{name}: missing columns {sorted(missing)}')
        return TaskData(frame,contract)
