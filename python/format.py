import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
import os

A = pd.read_csv(f'{Path(__file__).parent.parent}/output/A_geo.csv', index_col=False, dtype=str)
B = pd.read_csv(f'{Path(__file__).parent.parent}/output/B_geo.csv', index_col=False, dtype=str)
project_code = pd.read_csv(f'{Path(__file__).parent.parent}/output/project_code.csv', index_col=False, dtype=str)

A['project_id'] = A['project_name']
project_code.columns = ['project_code', 'program_name']
A.project_id = A.project_id.apply(lambda x: x.strip())
B.project_id = B.project_id.apply(lambda x: x.strip())
A.drop_duplicates(keep='first', inplace=True)
B.drop_duplicates(keep='first', inplace=True)
print(f'A.shape: {A.shape}, B.shape: {B.shape}')


project_code.project_code = project_code.project_code.apply(lambda x: x.strip())
df = pd.concat([A,B], sort=False)

df['join_key'] = df['project_id']\
                        .apply(lambda x: 'Targeted Code Enforcement'
                                        if 'Targeted Code Enforcement' in x else x)
project_code['join_key'] = project_code['program_name']\
                        .apply(lambda x: 'Targeted Code Enforcement'
                                        if 'Targeted Code Enforcement' in x else x)
df = df.merge(project_code[['program_name', 'project_code', 'join_key']],
                         how='left', left_on='join_key', right_on='join_key')

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
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index=False)

## Backfilling using pluto
df = pd.read_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index_col=False, dtype=str)
def get_boroct(bbl):
    con = create_engine(os.environ.get('EDM_DATA', ''))
    r = con.execute(f'''
            select borocode||lpad(split_part(ct2010,'.',1), 4,'0')||rpad(split_part(ct2010,'.',2),2,'0')
            from dcp_pluto."20v2" where bbl = \'{bbl}\'
        ''').fetchall()
    if len(r) == 0:
        return np.nan
    else:
        result = r[0][0]
        return result

df.loc[(df['BORO CT'].isna())&(~df.BBL.isna()), 'BORO CT'] = df.loc[(df['BORO CT'].isna())&(~df.BBL.isna()), 'BBL'].apply(get_boroct)
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index=False)

## Adding eligibility field
df = pd.read_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index_col=False, dtype=str)
eligibility = pd.read_csv(f'{Path(__file__).parent.parent}/data/CDBG_census_tract.csv', index_col=False, dtype=str)
df = df.merge(eligibility[['BoroCT', 'Eligibility']], how='left', left_on='BORO CT', right_on='BoroCT')
df = df.drop(columns=['BoroCT'],  axis=1)
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020_FINAL.csv', index=False)