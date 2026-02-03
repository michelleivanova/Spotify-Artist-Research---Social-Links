#!/usr/bin/env python3
"""
Script to find contact information (emails and social media) for playlist owners
from the Holiday Playlist Apple Music.csv file.
"""

import pandas as pd
import re
from collections import defaultdict

def clean_owner_name_for_handle(owner_name):
    """Clean owner name to create a likely social media handle"""
    if pd.isna(owner_name):
        return None
    
    # Convert to lowercase and remove special characters
    clean = str(owner_name).lower()
    # Remove common words
    clean = re.sub(r'\s+(music|records|magazine|recordings)\s*$', '', clean)
    # Remove spaces and special characters
    clean = re.sub(r'[^a-z0-9_]', '', clean)
    return clean

def construct_social_urls(owner_name):
    """Construct likely social media URLs and email patterns for owner"""
    handle = clean_owner_name_for_handle(owner_name)
    
    if not handle:
        return {}
    
    # Construct URLs
    urls = {
        'owner_name': owner_name,
        'instagram_url': f"https://www.instagram.com/{handle}",
        'instagram_handle': handle,
        'tiktok_url': f"https://www.tiktok.com/@{handle}",
        'tiktok_handle': handle,
        'youtube_url': f"https://www.youtube.com/@{handle}",
        'youtube_channel_id': f"@{handle}",
        'twitter_url': f"https://twitter.com/{handle}",
        'twitter_handle': handle,
        'soundcloud_url': f"https://soundcloud.com/{handle}",
        'soundcloud_handle': handle,
        'facebook_url': f"https://www.facebook.com/{handle}",
        'website_url': f"https://{handle}.com",
        'email_pattern_1': f"contact@{handle}.com",
        'email_pattern_2': f"info@{handle}.com",
        'email_pattern_3': f"hello@{handle}.com",
        'email_pattern_4': f"support@{handle}.com"
    }
    
    return urls

def main():
    # Read the Holiday playlist CSV
    print("Reading Holiday Playlist Apple Music.csv...")
    df = pd.read_csv('Holiday Playlist Apple Music.csv')
    
    print(f"\nTotal playlists: {len(df):,}")
    
    # Get unique owner names
    unique_owners = df['Owner Name'].unique()
    print(f"Unique playlist owners: {len(unique_owners)}")
    
    # Create a list to store owner contact information
    owner_contacts = []
    
    print("\nGenerating contact information for playlist owners...\n")
    
    for owner in sorted(unique_owners):
        if pd.isna(owner):
            continue
        
        # Count how many playlists this owner has
        playlist_count = len(df[df['Owner Name'] == owner])
        
        # Construct social media URLs
        contact_info = construct_social_urls(owner)
        contact_info['playlist_count'] = playlist_count
        
        owner_contacts.append(contact_info)
        
        print(f"✓ {owner} ({playlist_count} playlists)")
    
    # Create DataFrame from owner contacts
    contacts_df = pd.DataFrame(owner_contacts)
    
    # Reorder columns
    column_order = [
        'owner_name',
        'playlist_count',
        'email_pattern_1',
        'email_pattern_2',
        'email_pattern_3',
        'email_pattern_4',
        'instagram_url',
        'instagram_handle',
        'tiktok_url',
        'tiktok_handle',
        'youtube_url',
        'youtube_channel_id',
        'soundcloud_url',
        'soundcloud_handle',
        'twitter_url',
        'twitter_handle',
        'facebook_url',
        'website_url'
    ]
    
    contacts_df = contacts_df[column_order]
    
    # Save to CSV
    output_csv = 'playlist_owners_contact_info.csv'
    contacts_df.to_csv(output_csv, index=False)
    print(f"\n✓ Saved contact information to {output_csv}")
    
    # Save to Excel
    output_xlsx = 'playlist_owners_contact_info.xlsx'
    contacts_df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"✓ Saved contact information to {output_xlsx}")
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total unique playlist owners: {len(contacts_df)}")
    print(f"Total playlists in dataset: {len(df):,}")
    print(f"\nGenerated contact patterns for each owner:")
    print(f"  - 4 email patterns (contact@, info@, hello@, support@)")
    print(f"  - Instagram URL and handle")
    print(f"  - TikTok URL and handle")
    print(f"  - YouTube URL and channel ID")
    print(f"  - Twitter/X URL and handle")
    print(f"  - SoundCloud URL and handle")
    print(f"  - Facebook URL")
    print(f"  - Website URL pattern")
    
    print(f"\n{'='*80}")
    print("TOP 10 PLAYLIST OWNERS BY PLAYLIST COUNT")
    print(f"{'='*80}")
    top_owners = contacts_df.nlargest(10, 'playlist_count')[['owner_name', 'playlist_count', 'email_pattern_1']]
    print(top_owners.to_string(index=False))
    
    print(f"\n{'='*80}")
    print("IMPORTANT NOTE")
    print(f"{'='*80}")
    print("These are PREDICTED/CONSTRUCTED contact patterns based on owner names.")
    print("They may not be accurate. You should:")
    print("  1. Verify each URL/email by visiting the websites")
    print("  2. Use email verification tools to check if emails are valid")
    print("  3. Manually research official contact information")
    print("  4. For Apple Music teams, contact Apple Music directly")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
