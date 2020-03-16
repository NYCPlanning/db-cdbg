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
col_names = {'project_id': 'Project_name',
             'units':'Units',
             'units_assisted_with_cd':'Units_assisted',
             'boro':'Borocode',
             'address': 'Address',
             'input_type_code':'Type',
             'tax_block':'Block', 
             'tax_lot':'Lot',
             'geo_house_number':'GBAT_num',
             'geo_street_name':'GBAT_street',
             'geo_stcode':'GBAT_streetnum',
             'geo_block':'GBAT_block',
             'geo_lot':'GBAT_lot',
             'geo_bbl':'GBAT_BBL',  
             'geo_censtract':'GBAT_tract', 
             'geo_borough_code':'GBAT_boro',
             'boroct': 'GBAT_borotract',
             'geo_commboard':'GBAT_commdist', 
             'geo_council': 'GBAT_councildist',
             'geo_cong':'GBAT_congdist',
             'geo_bin':'GBAT_BIN',
             'bbl':'BBL'}

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

df.loc[(df['GBAT_borotract'].isna())&(~df.BBL.isna()), 'GBAT_borotract'] = df.loc[(df['GBAT_borotract'].isna())&(~df.BBL.isna()), 'BBL'].apply(get_boroct)
df.drop(columns = ['BBL'], inplace=True)
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index=False)

## Adding census tract fields
df = pd.read_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020.csv', index_col=False, dtype=str)
eligibility = pd.read_csv(f'{Path(__file__).parent.parent}/data/CDBG_census_tract.csv', index_col=False, dtype=str)
df = df.merge(eligibility[['BoroCT', 'Eligibility', 'TotalPop', 'LowMod_Population', 'Res_pct']], how='left', left_on='GBAT_borotract', right_on='BoroCT')
df = df.drop(columns=['BoroCT'],  axis=1).rename(columns = {'TotalPop':'Total_Persons',
                                                            'LowMod_Population':'LowMod_Persons',
                                                            'Res_pct':'Per_Residential',
                                                            'Eligibility':'CD_Eligibility'})
df.to_csv(f'{Path(__file__).parent.parent}/output/CDBG_2020_FINAL.csv', index=False)