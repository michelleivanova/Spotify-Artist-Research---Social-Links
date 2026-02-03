import pandas as pd
from openpyxl import load_workbook
import os

# File paths
source_file = 'Soundcharts Pulled-Out Data/DJProducers.csv'
final_csv = 'Final_Social Links/dj_producers_final_enriched.csv'
final_xlsx = 'Final_Social Links/dj_producers_final_enriched.xlsx'

print("Loading filtered source data from Soundcharts...")
# Read the already-filtered source CSV
df_source = pd.read_csv(source_file, low_memory=False)
print(f"Source data (filtered): {len(df_source)} rows")
print(f"Source columns: {len(df_source.columns)}")

print("\nLoading current final enriched data...")
# Read the current final CSV with social links
df_final = pd.read_csv(final_csv, low_memory=False)
print(f"Current final data: {len(df_final)} rows")
print(f"Final columns: {len(df_final.columns)}")

# Get the list of artists from the filtered source
source_artists = set(df_source['Artist'].str.strip().str.lower())
print(f"\nUnique artists in filtered source: {len(source_artists)}")

# Filter the final data to keep only artists that exist in the source
print("\nFiltering final data to match source artists...")
df_final['Artist_lower'] = df_final['Artist'].str.strip().str.lower()
df_filtered_final = df_final[df_final['Artist_lower'].isin(source_artists)].copy()
df_filtered_final = df_filtered_final.drop('Artist_lower', axis=1)

print(f"Filtered final data: {len(df_filtered_final)} rows")
print(f"Artists removed: {len(df_final) - len(df_filtered_final)}")

# Save the filtered data back to CSV
print(f"\nSaving filtered data to {final_csv}...")
df_filtered_final.to_csv(final_csv, index=False)
print("CSV file updated successfully!")

# Update the Excel file
print(f"\nUpdating Excel file {final_xlsx}...")
try:
    # Create a new Excel file from the filtered CSV
    with pd.ExcelWriter(final_xlsx, engine='openpyxl') as writer:
        df_filtered_final.to_excel(writer, sheet_name='DJ Producers', index=False)
    
    print("Excel file updated successfully!")
    
except Exception as e:
    print(f"Error updating Excel file: {e}")
    print("CSV file was updated successfully, but Excel update failed.")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Source file (filtered): {source_file} ({len(df_source)} rows)")
print(f"Final file (before): {len(df_final)} rows")
print(f"Final file (after): {len(df_filtered_final)} rows")
print(f"Artists removed: {len(df_final) - len(df_filtered_final)}")
print(f"Match rate: {len(df_filtered_final)/len(df_source)*100:.1f}%")
print("="*60)
print("\nFiltering complete! The Excel file now contains only artists")
print("that exist in the filtered Soundcharts DJProducers.csv file.")
