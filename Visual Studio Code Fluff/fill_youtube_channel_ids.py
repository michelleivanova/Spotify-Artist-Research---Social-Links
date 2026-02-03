#!/usr/bin/env python3
"""
Fill in missing YouTube channel IDs from YouTube URLs in female_singers_final.xlsx
"""

import pandas as pd
import re

def extract_youtube_channel_id(youtube_url):
    """Extract YouTube channel ID from URL"""
    if pd.isna(youtube_url) or not youtube_url:
        return None
    
    url_str = str(youtube_url).strip()
    
    # Pattern for /channel/ URLs (actual channel IDs)
    channel_match = re.search(r'/channel/([a-zA-Z0-9_-]+)', url_str)
    if channel_match:
        return channel_match.group(1)
    
    # Pattern for @handle URLs (new YouTube format)
    handle_match = re.search(r'/@([a-zA-Z0-9_.-]+)', url_str)
    if handle_match:
        return f"@{handle_match.group(1)}"
    
    # Pattern for /c/ custom URLs
    custom_match = re.search(r'/c/([a-zA-Z0-9_-]+)', url_str)
    if custom_match:
        return f"c/{custom_match.group(1)}"
    
    # Pattern for /user/ URLs
    user_match = re.search(r'/user/([a-zA-Z0-9_-]+)', url_str)
    if user_match:
        return f"user/{user_match.group(1)}"
    
    return None

def main():
    # Read the Excel file
    input_file = 'Final_Social Links/female_singers_final.xlsx'
    output_file = 'Final_Social Links/female_singers_final_updated.xlsx'
    
    print(f"Reading {input_file}...\n")
    df = pd.read_excel(input_file)
    
    print(f"Total rows: {len(df)}")
    print(f"YouTube URLs present: {df['youtube_url'].notna().sum()}")
    print(f"YouTube channel IDs before: {df['youtube_channel_id'].notna().sum()}\n")
    
    # Extract channel IDs
    filled_count = 0
    failed_urls = []
    
    for idx, row in df.iterrows():
        if pd.notna(row['youtube_url']) and pd.isna(row['youtube_channel_id']):
            channel_id = extract_youtube_channel_id(row['youtube_url'])
            if channel_id:
                df.at[idx, 'youtube_channel_id'] = channel_id
                filled_count += 1
                if filled_count <= 10:  # Show first 10 examples
                    print(f"✓ {row['Artist']:30s} -> {channel_id}")
            else:
                failed_urls.append({
                    'Artist': row['Artist'],
                    'URL': row['youtube_url']
                })
    
    print(f"\n{'='*80}")
    print(f"RESULTS")
    print(f"{'='*80}")
    print(f"YouTube channel IDs filled: {filled_count}")
    print(f"YouTube channel IDs after: {df['youtube_channel_id'].notna().sum()}")
    print(f"Failed to extract: {len(failed_urls)}")
    
    if failed_urls:
        print(f"\nURLs that couldn't be parsed:")
        for item in failed_urls[:10]:  # Show first 10
            print(f"  {item['Artist']:30s}: {item['URL']}")
    
    # Save the updated file
    print(f"\nSaving to {output_file}...")
    df.to_excel(output_file, index=False)
    print("✓ File saved successfully!")
    
    # Also save as CSV for easier viewing
    csv_output = output_file.replace('.xlsx', '.csv')
    df.to_csv(csv_output, index=False)
    print(f"✓ Also saved as CSV: {csv_output}")

if __name__ == "__main__":
    main()
