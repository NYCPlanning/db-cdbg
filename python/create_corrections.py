import pandas as pd
import numpy as np
from pathlib import Path

A = pd.read_csv(f'{Path(__file__).parent.parent}/output/A_failure.csv', dtype=str, index_col = False)
B = pd.read_csv(f'{Path(__file__).parent.parent}/output/B_failure.csv', dtype=str, index_col = False)
A['project_id'] = A['project_name']
df = pd.concat([A,B], sort=False)

df = df[['uid', 'project_id', 'input_type_code','geo_grc', 'geo_grc2', 'geo_message', 'geo_reason_code', 'bbl', 'hnum', 'sname','boro']]
df['omb_bbl'] = ''
df['omb_house_number'] = ''
df['omb_street_name'] = ''
df['omb_borough'] = ''
df.to_csv(f'{Path(__file__).parent.parent}/output/corrections.csv', index = False)
