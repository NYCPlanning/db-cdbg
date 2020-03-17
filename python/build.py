import pandas as pd 
import numpy as np
import os
import json
from pathlib import Path
from utils import get_hnum, get_sname, geocode_a, geocode_bbl
from multiprocessing import Pool, cpu_count

# # Load data
# file_path = f'{Path(__file__).parent.parent}/data/Merged Text File for 2019.xlsx'
# A = pd.read_excel(file_path, sheet_name='Type A file', skiprows=3, dtype=str)
# B = pd.read_excel(file_path, sheet_name='Type B File', skiprows=3, dtype=str)
# # project_code = pd.read_excel(file_path, sheet_name='Project Codes')

# # Change column names from e.g. Borough Code tp borough_code
# def format_colnames(df):
#     new_names = {}
#     for i in df.columns:
#         new_names[i] = i.lower().strip().replace(' ', '_')
#     return df.rename(columns=new_names)

# A = format_colnames(A)
# B = format_colnames(B)
# # project_code = format_colnames(project_code)

# # Remove nan
# A=A.dropna(axis=0, how='all')
# B=B.dropna(axis=0, how='all')

# # Float to int to str
# A.borough_code = A.borough_code.astype(int).astype(str)
# B.borough_code = B.borough_code.astype(int).astype(str)
# B.tax_block = B.tax_block.astype(int).astype(str)
# B.tax_lot = B.tax_lot.astype(int).astype(str)

# # Parse address
# A['hnum'] = A['address'].apply(get_hnum)
# A['sname'] = A['address'].apply(get_sname)
# A['project_id'] = A['project_name']
# A['boro'] = A['borough_code']

# # # Create unique identifier
# A['uid'] = list(map(lambda x,y: str(x)+', '+str(y), A['address'], A['borough_code']))

# # # Make BBL BOROUGH-1 BLOCK-5 LOT-4
# B['bbl'] = list(map(lambda boro, block, lot: 
#                 boro.zfill(1)+block.zfill(5)+lot.zfill(4), 
#                 B.borough_code, B.tax_block, B.tax_lot))
# B['uid'] = B['bbl'].astype(str)

# A.to_csv(f'{Path(__file__).parent.parent}/output/A.csv', index = False)
# B.to_csv(f'{Path(__file__).parent.parent}/output/B.csv', index = False)
# # project_code.to_csv(f'{Path(__file__).parent.parent}/output/project_code.csv', index = False)


A = pd.read_csv(f'{Path(__file__).parent.parent}/output/A.csv', dtype=str)
A['bbl'] = np.nan
B = pd.read_csv(f'{Path(__file__).parent.parent}/output/B.csv', dtype=str)
C = pd.read_csv(f'{Path(__file__).parent.parent}/output/corrections_input.csv', dtype=str)
B['boro'] = B['bbl'].apply(lambda x: x[0])

# Extract type-A corrections, and sort into address updates or BBL updates
C1 = C.loc[C.input_type_code == 'A']
C1_1 = C1[(C1.omb_house_number.notnull())&(C1.omb_street_name.notnull())&(C1.omb_borough.notnull())].reset_index()
C1_2 = C1[(C1.omb_bbl.notnull())].reset_index()

# Extract type-B corrections
C2 = C.loc[C.input_type_code == 'B']
C2 = C2[C2.omb_bbl.notnull()].reset_index()

# Apply type-A address updates
for i in range(C1_1.shape[0]):
    A.loc[A.uid == C1_1.loc[i, 'uid'], 'hnum'] = C1_1.loc[i,'omb_house_number']
    A.loc[A.uid == C1_1.loc[i, 'uid'], 'sname'] = C1_1.loc[i, 'omb_street_name']
    A.loc[A.uid == C1_1.loc[i, 'uid'], 'boro'] = C1_1.loc[i, 'omb_borough']

# Apply type-A BBL updates
for i in range(C1_2.shape[0]):
    A.loc[A.uid == C1_2.loc[i, 'uid'], 'bbl'] = C1_2.loc[i,'omb_bbl']

# Apply type-B BBL updates
for i in range(C2.shape[0]):
    B.loc[B.uid == C2.loc[i, 'uid'], 'bbl'] = C2.loc[i,'omb_bbl']


with Pool(processes=cpu_count()) as pool:
    it = pool.map(geocode_a, A.to_dict('records'))
A_geo = pd.DataFrame(it)

with Pool(processes=cpu_count()) as pool:
    it = pool.map(geocode_bbl, B.to_dict('records'))
B_geo = pd.DataFrame(it)

A_geo.to_csv(f'{Path(__file__).parent.parent}/output/A_geo.csv', index = False)
A_geo.loc[A_geo['status'] == 'failure', :]\
    .to_csv(f'{Path(__file__).parent.parent}/output/A_failure.csv', index = False)

B_geo.to_csv(f'{Path(__file__).parent.parent}/output/B_geo.csv', index = False)
B_geo.loc[B_geo['status'] == 'failure', :]\
    .to_csv(f'{Path(__file__).parent.parent}/output/B_failure.csv', index = False)
