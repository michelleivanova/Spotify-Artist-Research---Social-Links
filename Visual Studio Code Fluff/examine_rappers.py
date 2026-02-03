#!/usr/bin/env python3
"""
Script to examine the Rappers.xlsx file structure
"""

import pandas as pd
import sys

def main():
    try:
        # Read the Excel file
        file_path = 'Soundcharts Pulled-Out Data/Rappers.xlsx'
        print(f"Reading {file_path}...\n")
        
        df = pd.read_excel(file_path)
        
        print(f"{'='*80}")
        print(f"FILE STRUCTURE")
        print(f"{'='*80}")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        
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
                print(f"  {col:40s}: {filled:6,}/{total:6,} ({pct:5.1f}%)")
        else:
            print("No social media URL columns found.")
            print("Will need to add social media columns.")
        
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
