#!/usr/bin/env python3
"""
Script to examine the female_singers_final.xlsx file in Final_Social Links directory
"""

import pandas as pd
import sys

def main():
    try:
        # Read the Excel file
        file_path = 'Final_Social Links/female_singers_final.xlsx'
        print(f"Reading {file_path}...")
        
        df = pd.read_excel(file_path)
        
        print(f"\n{'='*80}")
        print(f"FILE STRUCTURE")
        print(f"{'='*80}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        
        print(f"\n{'='*80}")
        print(f"COLUMN NAMES")
        print(f"{'='*80}")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        print(f"\n{'='*80}")
        print(f"FIRST 5 ROWS")
        print(f"{'='*80}")
        print(df.head())
        
        print(f"\n{'='*80}")
        print(f"DATA TYPES")
        print(f"{'='*80}")
        print(df.dtypes)
        
        print(f"\n{'='*80}")
        print(f"MISSING DATA SUMMARY")
        print(f"{'='*80}")
        missing_data = df.isnull().sum()
        missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
        if len(missing_data) > 0:
            for col, count in missing_data.items():
                percentage = (count / len(df)) * 100
                print(f"{col}: {count} ({percentage:.1f}%)")
        else:
            print("No missing data found!")
        
        print(f"\n{'='*80}")
        print(f"BASIC STATISTICS")
        print(f"{'='*80}")
        print(df.describe(include='all'))
        
    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
