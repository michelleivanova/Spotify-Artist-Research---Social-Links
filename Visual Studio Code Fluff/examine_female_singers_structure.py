#!/usr/bin/env python3
import pandas as pd

# Read the female singers file
df = pd.ExcelFile('Final_Social Links/female_singers_final.xlsx')
print('Sheet names:', df.sheet_names)
print()

for sheet in df.sheet_names:
    print(f'Sheet: {sheet}')
    data = pd.read_excel('Final_Social Links/female_singers_final.xlsx', sheet_name=sheet)
    print(f'  Rows: {len(data)}')
    print(f'  Columns: {len(data.columns)}')
    print(f'  Column names (first 20): {list(data.columns[:20])}')
    if len(data.columns) > 20:
        print(f'  Column names (last 20): {list(data.columns[-20:])}')
    print()
