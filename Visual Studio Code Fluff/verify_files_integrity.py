#!/usr/bin/env python3
"""
Verify the integrity and quality of the enriched files
"""

import pandas as pd
import re

def verify_url_format(url, platform):
    """Verify URL format is correct for the platform"""
    if pd.isna(url):
        return True, "N/A"
    
    url = str(url)
    patterns = {
        'instagram': r'^https://www\.instagram\.com/[\w.-]+$',
        'tiktok': r'^https://www\.tiktok\.com/@[\w.-]+$',
        'youtube': r'^https://www\.youtube\.com/@[\w.-]+$',
        'twitter': r'^https://twitter\.com/[\w.-]+$',
        'soundcloud': r'^https://soundcloud\.com/[\w.-]+$',
        'facebook': r'^https://www\.facebook\.com/[\w.-]+$'
    }
    
    if platform in patterns:
        if re.match(patterns[platform], url):
            return True, "Valid"
        else:
            return False, f"Invalid format: {url}"
    
    return True, "Unknown platform"

def verify_file(file_path, file_name):
    """Verify a single file's integrity"""
    print(f"\n{'='*80}")
    print(f"VERIFYING: {file_name}")
    print(f"{'='*80}")
    
    try:
        # Try to read the file
        print(f"Reading {file_path}...")
        df = pd.read_excel(file_path)
        
        print(f"✓ File opened successfully")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        
        # Check for required columns
        social_columns = ['instagram_url', 'tiktok_url', 'youtube_url', 
                         'twitter_url', 'soundcloud_url', 'facebook_url']
        
        missing_cols = [col for col in social_columns if col not in df.columns]
        if missing_cols:
            print(f"⚠ Missing columns: {missing_cols}")
        else:
            print(f"✓ All social media columns present")
        
        # Check coverage
        print(f"\nCoverage:")
        for col in social_columns:
            if col in df.columns:
                filled = df[col].notna().sum()
                pct = (filled / len(df)) * 100
                print(f"  {col:20s}: {filled:6,}/{len(df):,} ({pct:5.1f}%)")
        
        # Sample URL validation
        print(f"\nURL Format Validation (sample of 100 rows):")
        sample_size = min(100, len(df))
        sample_df = df.sample(n=sample_size, random_state=42)
        
        url_checks = {
            'instagram_url': 'instagram',
            'tiktok_url': 'tiktok',
            'youtube_url': 'youtube',
            'twitter_url': 'twitter',
            'soundcloud_url': 'soundcloud',
            'facebook_url': 'facebook'
        }
        
        for col, platform in url_checks.items():
            if col in sample_df.columns:
                valid_count = 0
                invalid_urls = []
                
                for url in sample_df[col].dropna():
                    is_valid, msg = verify_url_format(url, platform)
                    if is_valid:
                        valid_count += 1
                    else:
                        invalid_urls.append(msg)
                
                total_checked = len(sample_df[col].dropna())
                if total_checked > 0:
                    pct = (valid_count / total_checked) * 100
                    status = "✓" if pct == 100 else "⚠"
                    print(f"  {status} {col:20s}: {valid_count}/{total_checked} valid ({pct:.1f}%)")
                    
                    if invalid_urls and len(invalid_urls) <= 5:
                        for inv_url in invalid_urls[:5]:
                            print(f"      - {inv_url}")
        
        # Check for artists that didn't get enriched
        if 'Artist' in df.columns:
            missing_all = df[
                (df['instagram_url'].isna()) & 
                (df['tiktok_url'].isna()) & 
                (df['youtube_url'].isna())
            ]
            
            if len(missing_all) > 0:
                print(f"\n⚠ Artists with NO social links: {len(missing_all)}")
                print(f"  Sample (first 5):")
                for idx, row in missing_all.head(5).iterrows():
                    print(f"    - {row['Artist']}")
            else:
                print(f"\n✓ All artists have at least one social link")
        
        # Check for duplicate artists
        if 'Artist' in df.columns:
            duplicates = df[df.duplicated(subset=['Artist'], keep=False)]
            if len(duplicates) > 0:
                print(f"\n⚠ Duplicate artists found: {len(duplicates)}")
                print(f"  Sample (first 5):")
                for artist in duplicates['Artist'].unique()[:5]:
                    print(f"    - {artist}")
            else:
                print(f"\n✓ No duplicate artists")
        
        return True
        
    except Exception as e:
        print(f"✗ Error reading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def verify_multisheet_file(file_path, file_name):
    """Verify a multi-sheet Excel file"""
    print(f"\n{'='*80}")
    print(f"VERIFYING MULTI-SHEET FILE: {file_name}")
    print(f"{'='*80}")
    
    try:
        # Read all sheets
        print(f"Reading {file_path}...")
        xl_file = pd.ExcelFile(file_path)
        
        print(f"✓ File opened successfully")
        print(f"  Sheets found: {xl_file.sheet_names}")
        
        # Verify expected sheets
        expected_sheets = ['Original Data', 'Social Links']
        for sheet in expected_sheets:
            if sheet in xl_file.sheet_names:
                print(f"  ✓ Sheet '{sheet}' present")
                
                # Read the sheet
                df = pd.read_excel(file_path, sheet_name=sheet)
                print(f"    - Rows: {len(df):,}")
                print(f"    - Columns: {len(df.columns)}")
            else:
                print(f"  ✗ Sheet '{sheet}' MISSING")
        
        # Verify Social Links sheet in detail
        if 'Social Links' in xl_file.sheet_names:
            df_social = pd.read_excel(file_path, sheet_name='Social Links')
            
            print(f"\nSocial Links Sheet Details:")
            social_columns = ['instagram_url', 'tiktok_url', 'youtube_url', 
                            'twitter_url', 'soundcloud_url', 'facebook_url']
            
            for col in social_columns:
                if col in df_social.columns:
                    filled = df_social[col].notna().sum()
                    pct = (filled / len(df_social)) * 100
                    print(f"  {col:20s}: {filled:6,}/{len(df_social):,} ({pct:5.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"✗ Error reading file: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("="*80)
    print("FILE INTEGRITY VERIFICATION")
    print("="*80)
    
    files_to_verify = [
        ('Final_Social Links/female_singers_final_updated.xlsx', 'Female Singers (Updated)', False),
        ('Final_Social Links/dj_producers_final.xlsx', 'DJ Producers', True),
        ('Final_Social Links/rappers_final.xlsx', 'Rappers', True),
    ]
    
    results = []
    
    for file_path, file_name, is_multisheet in files_to_verify:
        if is_multisheet:
            success = verify_multisheet_file(file_path, file_name)
        else:
            success = verify_file(file_path, file_name)
        
        results.append((file_name, success))
    
    # Summary
    print(f"\n{'='*80}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*80}")
    
    for file_name, success in results:
        status = "✓ PASSED" if success else "✗ FAILED"
        print(f"{status}: {file_name}")
    
    all_passed = all(success for _, success in results)
    
    if all_passed:
        print(f"\n✓ All files verified successfully!")
    else:
        print(f"\n⚠ Some files failed verification. Please review the errors above.")

if __name__ == "__main__":
    main()
