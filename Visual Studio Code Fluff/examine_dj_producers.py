#!/usr/bin/env python3
"""
Script to examine the DJProducers.xlsx file structure
"""

import pandas as pd
import sys

def main():
    try:
        # Read the Excel file
        file_path = 'Soundcharts Pulled-Out Data/DJProducers.xlsx'
        print(f"Reading {file_path}...\n")
        
        df = pd.read_excel(file_path)
        
        print(f"{'='*80}")
        print(f"FILE STRUCTURE")
        print(f"{'='*80}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        
        print(f"\n{'='*80}")
        print(f"COLUMN NAMES")
        print(f"{'='*80}")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:3d}. {col}")
        
        print(f"\n{'='*80}")
        print(f"FIRST 5 ROWS")
        print(f"{'='*80}")
        print(df.head())
        
        print(f"\n{'='*80}")
        print(f"SOCIAL MEDIA COLUMNS CHECK")
        print(f"{'='*80}")
        
        # Check for social media related columns
        social_keywords = ['instagram', 'tiktok', 'youtube', 'soundcloud', 'twitter', 'facebook', 'website', 'spotify']
        social_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in social_keywords)]
        
        if social_cols:
            print("Social media columns found:")
            for col in social_cols:
                filled = df[col].notna().sum()
                total = len(df)
                pct = (filled / total) * 100
                print(f"  {col:40s}: {filled:4d}/{total:4d} ({pct:5.1f}%)")
        else:
            print("No social media columns found in this file.")
            print("\nThis file may need social media data to be added.")
        
    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
