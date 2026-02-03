#!/usr/bin/env python3
"""
Script to filter user-generated Christmas playlists from Holiday Playlist Apple Music.csv.
Filters out editorial playlists (Apple Music, etc.) and keeps only user-generated ones.
"""

import pandas as pd

def is_editorial_owner(owner_name):
    """
    Determine if a playlist owner is editorial (not user-generated).
    Returns True if editorial, False if user-generated.
    """
    if pd.isna(owner_name):
        return True
    
    owner_lower = str(owner_name).lower()
    
    # Editorial keywords - these are NOT user-generated
    editorial_keywords = [
        'apple music',
        'spotify',
        'amazon music',
        'youtube music',
        'tidal',
        'deezer',
        'pandora',
        'editorial',
        'official',
        'curated'
    ]
    
    # Check if owner contains any editorial keywords
    for keyword in editorial_keywords:
        if keyword in owner_lower:
            return True
    
    # These are user-generated curators
    return False

def main():
    # Read the Holiday playlist CSV
    print("Reading Holiday Playlist Apple Music.csv...")
    df = pd.read_csv('Holiday Playlist Apple Music.csv')
    
    print(f"\nTotal playlists in file: {len(df):,}")
    print(f"Unique owners: {df['Owner Name'].nunique()}")
    
    # Filter for user-generated playlists only
    print("\nFiltering for user-generated playlists...")
    df['is_editorial'] = df['Owner Name'].apply(is_editorial_owner)
    
    user_generated_df = df[~df['is_editorial']].copy()
    editorial_df = df[df['is_editorial']].copy()
    
    print(f"\nEditorial playlists (excluded): {len(editorial_df):,}")
    print(f"User-generated playlists (kept): {len(user_generated_df):,}")
    
    # Show editorial owners being excluded
    print(f"\n{'='*80}")
    print("EDITORIAL OWNERS EXCLUDED:")
    print(f"{'='*80}")
    editorial_owners = editorial_df['Owner Name'].value_counts()
    for owner, count in editorial_owners.items():
        print(f"  {owner}: {count:,} playlists")
    
    # Show user-generated owners being kept
    print(f"\n{'='*80}")
    print("USER-GENERATED OWNERS KEPT:")
    print(f"{'='*80}")
    user_owners = user_generated_df['Owner Name'].value_counts()
    for owner, count in user_owners.items():
        print(f"  {owner}: {count:,} playlists")
    
    # Remove the helper column
    user_generated_df = user_generated_df.drop('is_editorial', axis=1)
    
    # Save filtered data
    output_csv = 'user_generated_christmas_playlists.csv'
    user_generated_df.to_csv(output_csv, index=False)
    print(f"\n✓ Saved user-generated playlists to {output_csv}")
    
    output_xlsx = 'user_generated_christmas_playlists.xlsx'
    user_generated_df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"✓ Saved user-generated playlists to {output_xlsx}")
    
    # Now create contact information for user-generated owners only
    print(f"\n{'='*80}")
    print("GENERATING CONTACT INFORMATION FOR USER-GENERATED OWNERS")
    print(f"{'='*80}")
    
    unique_user_owners = user_generated_df['Owner Name'].unique()
    
    owner_contacts = []
    
    for owner in sorted(unique_user_owners):
        if pd.isna(owner):
            continue
        
        playlist_count = len(user_generated_df[user_generated_df['Owner Name'] == owner])
        
        # Clean owner name for handles
        handle = str(owner).lower()
        handle = re.sub(r'\s+(music|records|magazine|recordings)\s*$', '', handle)
        handle = re.sub(r'[^a-z0-9_]', '', handle)
        
        contact_info = {
            'owner_name': owner,
            'playlist_count': playlist_count,
            'email_pattern_1': f"contact@{handle}.com",
            'email_pattern_2': f"info@{handle}.com",
            'email_pattern_3': f"hello@{handle}.com",
            'email_pattern_4': f"support@{handle}.com",
            'instagram_url': f"https://www.instagram.com/{handle}",
            'instagram_handle': handle,
            'tiktok_url': f"https://www.tiktok.com/@{handle}",
            'tiktok_handle': handle,
            'youtube_url': f"https://www.youtube.com/@{handle}",
            'youtube_channel_id': f"@{handle}",
            'soundcloud_url': f"https://soundcloud.com/{handle}",
            'soundcloud_handle': handle,
            'twitter_url': f"https://twitter.com/{handle}",
            'twitter_handle': handle,
            'facebook_url': f"https://www.facebook.com/{handle}",
            'website_url': f"https://{handle}.com"
        }
        
        owner_contacts.append(contact_info)
        print(f"✓ {owner} ({playlist_count} playlists)")
    
    # Create DataFrame
    contacts_df = pd.DataFrame(owner_contacts)
    
    # Save contact information
    contacts_csv = 'user_generated_owners_contact_info.csv'
    contacts_df.to_csv(contacts_csv, index=False)
    print(f"\n✓ Saved contact info to {contacts_csv}")
    
    contacts_xlsx = 'user_generated_owners_contact_info.xlsx'
    contacts_df.to_excel(contacts_xlsx, index=False, engine='openpyxl')
    print(f"✓ Saved contact info to {contacts_xlsx}")
    
    # Final summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Total playlists analyzed: {len(df):,}")
    print(f"Editorial playlists excluded: {len(editorial_df):,}")
    print(f"User-generated playlists kept: {len(user_generated_df):,}")
    print(f"Unique user-generated owners: {len(unique_user_owners)}")
    print(f"\nFiles created:")
    print(f"  1. {output_csv} - Filtered playlist data")
    print(f"  2. {output_xlsx} - Filtered playlist data (Excel)")
    print(f"  3. {contacts_csv} - Owner contact information")
    print(f"  4. {contacts_xlsx} - Owner contact information (Excel)")
    
    print(f"\n{'='*80}")
    print("NEXT STEPS")
    print(f"{'='*80}")
    print("1. Review the user_generated_owners_contact_info.xlsx file")
    print("2. Verify email addresses using email verification tools")
    print("3. Check social media URLs manually to confirm they exist")
    print("4. Research official contact information for each curator")
    print("5. Reach out to playlist owners for collaboration opportunities")

if __name__ == "__main__":
    import re
    main()
