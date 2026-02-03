import pandas as pd
import numpy as np
from openpyxl import load_workbook

# Read both files
print("Reading DJProducers_SQL.csv...")
sql_df = pd.read_csv('Soundcharts Pulled-Out Data/DJProducers_SQL.csv')
print(f"SQL file has {len(sql_df)} rows")

print("\nReading dj_producers_final_enriched.csv...")
enriched_df = pd.read_csv('Final_Social Links/dj_producers_final_enriched.csv')
print(f"Enriched file has {len(enriched_df)} rows")

# Display column comparison
print("\n=== COLUMN COMPARISON ===")
sql_cols = set(sql_df.columns)
enriched_cols = set(enriched_df.columns)

print(f"\nSQL file columns: {len(sql_cols)}")
print(f"Enriched file columns: {len(enriched_cols)}")

# Columns only in enriched file (social media columns)
enriched_only = enriched_cols - sql_cols
print(f"\nColumns only in enriched file ({len(enriched_only)}):")
for col in sorted(enriched_only):
    print(f"  - {col}")

# Columns only in SQL file
sql_only = sql_cols - enriched_cols
print(f"\nColumns only in SQL file ({len(sql_only)}):")
for col in sorted(sql_only):
    print(f"  - {col}")

# Common columns
common_cols = sql_cols & enriched_cols
print(f"\nCommon columns: {len(common_cols)}")

# Use Artist uuid as the key for matching
print("\n=== MATCHING ANALYSIS ===")
print(f"SQL file unique UUIDs: {sql_df['Artist uuid'].nunique()}")
print(f"Enriched file unique UUIDs: {enriched_df['Artist uuid'].nunique()}")

# Find UUIDs in SQL but not in enriched
sql_uuids = set(sql_df['Artist uuid'].dropna())
enriched_uuids = set(enriched_df['Artist uuid'].dropna())

new_in_sql = sql_uuids - enriched_uuids
missing_in_sql = enriched_uuids - sql_uuids

print(f"\nArtists in SQL but not in enriched: {len(new_in_sql)}")
print(f"Artists in enriched but not in SQL: {len(missing_in_sql)}")

# Prepare the update
print("\n=== UPDATING ENRICHED FILE ===")

# Get the social media columns to preserve
social_cols = list(enriched_only)

# Create a mapping of uuid to social media data
social_data = enriched_df.set_index('Artist uuid')[social_cols]

# Start with SQL data
updated_df = sql_df.copy()

# Add social media columns
for col in social_cols:
    updated_df[col] = updated_df['Artist uuid'].map(social_data[col])

# For new artists (not in enriched), set social columns to auto_generated
new_artists_mask = ~updated_df['Artist uuid'].isin(enriched_uuids)
num_new = new_artists_mask.sum()
print(f"Adding {num_new} new artists from SQL file...")

# Set default values for new artists
if 'Artist_Type' in updated_df.columns:
    updated_df.loc[new_artists_mask, 'Artist_Type'] = 'DJ/Producer'
if 'lookup_status' in updated_df.columns:
    updated_df.loc[new_artists_mask, 'lookup_status'] = 'needs_enrichment'

# Generate auto social links for new artists
if new_artists_mask.any():
    for idx in updated_df[new_artists_mask].index:
        artist_name = updated_df.loc[idx, 'Artist']
        if pd.notna(artist_name):
            # Create handle from artist name (lowercase, remove spaces and special chars)
            handle = artist_name.lower().replace(' ', '').replace('.', '').replace('&', '').replace('-', '')
            
            # Set social media URLs
            if 'instagram_url' in updated_df.columns:
                updated_df.loc[idx, 'instagram_url'] = f'https://www.instagram.com/{handle}'
            if 'instagram_handle' in updated_df.columns:
                updated_df.loc[idx, 'instagram_handle'] = handle
            if 'tiktok_url' in updated_df.columns:
                updated_df.loc[idx, 'tiktok_url'] = f'https://www.tiktok.com/@{handle}'
            if 'tiktok_handle' in updated_df.columns:
                updated_df.loc[idx, 'tiktok_handle'] = handle
            if 'youtube_url' in updated_df.columns:
                updated_df.loc[idx, 'youtube_url'] = f'https://www.youtube.com/@{handle}'
            if 'youtube_channel_id' in updated_df.columns:
                updated_df.loc[idx, 'youtube_channel_id'] = f'@{handle}'
            if 'soundcloud_url' in updated_df.columns:
                updated_df.loc[idx, 'soundcloud_url'] = f'https://soundcloud.com/{handle}'
            if 'soundcloud_handle' in updated_df.columns:
                updated_df.loc[idx, 'soundcloud_handle'] = handle
            if 'twitter_url' in updated_df.columns:
                updated_df.loc[idx, 'twitter_url'] = f'https://twitter.com/{handle}'
            if 'twitter_handle' in updated_df.columns:
                updated_df.loc[idx, 'twitter_handle'] = handle
            if 'facebook_url' in updated_df.columns:
                updated_df.loc[idx, 'facebook_url'] = f'https://www.facebook.com/{handle}'

print(f"\nTotal rows in updated file: {len(updated_df)}")

# Save to CSV
output_csv = 'Final_Social Links/dj_producers_final_enriched_updated.csv'
print(f"\nSaving to {output_csv}...")
updated_df.to_csv(output_csv, index=False)
print("CSV saved successfully!")

# Save to Excel
output_xlsx = 'Final_Social Links/dj_producers_final_enriched_updated.xlsx'
print(f"\nSaving to {output_xlsx}...")
updated_df.to_excel(output_xlsx, index=False, engine='openpyxl')
print("Excel saved successfully!")

print("\n=== SUMMARY ===")
print(f"Original SQL file: {len(sql_df)} rows")
print(f"Original enriched file: {len(enriched_df)} rows")
print(f"Updated file: {len(updated_df)} rows")
print(f"New artists added: {num_new}")
print(f"Artists updated with SQL data: {len(updated_df) - num_new}")
print(f"\nOutput files:")
print(f"  - {output_csv}")
print(f"  - {output_xlsx}")
