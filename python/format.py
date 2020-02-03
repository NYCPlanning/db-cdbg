import pandas as pd
import numpy as np
from pathlib import Path

A = pd.read_csv(f'{Path(__file__).parent.parent}/output/A_geo.csv', index_col=False, dtype=str)
B = pd.read_csv(f'{Path(__file__).parent.parent}/output/B_geo.csv', index_col=False, dtype=str)
project_code = pd.read_csv(f'{Path(__file__).parent.parent}/output/project_code.csv', index_col=False, dtype=str)

A.project_id = A.project_id.apply(lambda x: x.strip())
B.project_id = B.project_id.apply(lambda x: x.strip())
A.drop_duplicates(keep='first', inplace=True)
B.drop_duplicates(keep='first', inplace=True)
print(f'A.shape: {A.shape}, B.shape: {B.shape}')


project_code.project_code = project_code.project_code.apply(lambda x: x.strip())
df = pd.concat([A,B], sort=False)
df = df.merge(project_code[['program_name', 'project_code']], how='left', left_on='project_id', right_on='project_code')
df.fillna('', inplace=True)
df['boroct'] = list(map(lambda boro, ct: boro+ct if ct!='' else '', df.boro, df.geo_censtract))
col_names = {'project_id': 'PROJCODE', 
             'units':'UNIT SUM', 
             'input_type_code':'CODE',
             'boro':'BORO',
             'address': 'ADDRESS',
             'geo_address':'ADDRESS GBAT',
             'geo_censtract':'CT GBAT', 
             'geo_borough_code':'BORO GBAT',
             'boroct': 'BORO CT',
             'geo_commboard':'COMM DIST GBAT', 
             'geo_council': 'COUNCIL DIST GBAT',
             'geo_cong':'CONG DIST GBAT',
             'tax_block':'BLOCK', 
             'tax_lot':'LOT',
             'geo_block':'BLOCK GBAT',
             'geo_lot':'LOT GBAT', 
             'geo_stcode':'STCODE GBAT', 
             'geo_bin':'BIN GBAT',
             'program_name':'PROJNAME',
             'geo_street_name':'STREET NAME GBAT',
             'geo_house_number':'HSNUM GBAT',
             'hnum': 'HSNUM',
             'sname': 'STNAME',
             'bbl':'BBL',
             'geo_bbl':'BBL GBAT'}

df = df.rename(columns=col_names)
df = df[list(col_names.values())]
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2019_half_done.csv', index=False)