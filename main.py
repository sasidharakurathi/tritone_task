import os
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time
import csv
import pickle
from dotenv import load_dotenv


# --- CONFIGURATION ---
load_dotenv()

SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')

TSV_FILE_PATH = 'unclaimedmusicalworkrightshares.tsv'
CACHE_FILE_PATH = 'isrc_cache.pkl'
OUTPUT_XLSX_PATH = 'tritone_task_output.xlsx'
ARTIST_URI = 'spotify:artist:06HL4z0CvFAxyc27GXpf02' 

# --- CACHED DATA LOADING FUNCTION ---

def load_or_create_isrc_set(tsv_path, cache_path):
    """
    Loads the ISRC set from a cache file if it exists.
    If not, it creates the set from the TSV and saves it to the cache.
    """
    # Check if the cache file already exists
    if os.path.exists(cache_path):
        print(f"Cache found! Loading ISRCs from {cache_path}...")
        with open(cache_path, 'rb') as f: # 'rb' = read bytes
            isrc_set = pickle.load(f)
        print(f"Successfully loaded {len(isrc_set)} ISRCs from cache.")
        return isrc_set
    
    # If cache does not exist, process the large file
    print("No cache found. Processing the large TSV file for the first time...")
    try:
        isrc_set = set()
        isrc_column_name = 'ISRC'
        
        with open(tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                isrc = row.get(isrc_column_name)
                if isrc:
                    isrc_set.add(isrc)
        
        # Save the newly created set to the cache file for next time
        print(f"Processing complete. Saving {len(isrc_set)} ISRCs to cache file: {cache_path}")
        with open(cache_path, 'wb') as f: # 'wb' = write bytes
            pickle.dump(isrc_set, f)
            
        return isrc_set
    except FileNotFoundError:
        print(f"Error: The file {tsv_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred while processing the TSV file: {e}")
        return None


def get_artist_catalog_fast(artist_uri):
    print(f"Connecting to Spotify and retrieving catalog for artist {artist_uri}...")
    try:
        auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
        sp = spotipy.Spotify(auth_manager=auth_manager)
        all_track_ids = []
        results = sp.artist_albums(artist_uri, album_type='album,single')
        albums = results['items']
        while results['next']:
            results = sp.next(results)
            albums.extend(results['items'])
        for album in albums:
            album_tracks_result = sp.album_tracks(album['id'])
            for track in album_tracks_result['items']:
                all_track_ids.append(track['id'])
        all_tracks_data = []
        print(f"Found {len(all_track_ids)} tracks. Fetching details in batches...")
        for i in range(0, len(all_track_ids), 50):
            chunk = all_track_ids[i:i+50]
            track_details_batch = sp.tracks(chunk)
            for track_details in track_details_batch['tracks']:
                if track_details:
                    isrc = track_details.get('external_ids', {}).get('isrc')
                    all_tracks_data.append({
                        'track_name': track_details['name'],
                        'album': track_details['album']['name'],
                        'release_date': track_details['album']['release_date'],
                        'isrc': isrc
                    })
        return pd.DataFrame(all_tracks_data)
    except Exception as e:
        print(f"An error occurred while fetching data from Spotify: {e}")
        return None

def main():
    """
    Main function to run the entire process.
    """
    # Step 1: Load ISRC set from cache or create it if it doesn't exist
    unclaimed_isrc_set = load_or_create_isrc_set(TSV_FILE_PATH, CACHE_FILE_PATH)
    if unclaimed_isrc_set is None:
        return

    # Step 2: Retrieve artist catalog from Spotify API
    artist_catalog_df = get_artist_catalog_fast(ARTIST_URI)
    if artist_catalog_df is None:
        return
        
    artist_catalog_df.dropna(subset=['isrc'], inplace=True)
    artist_catalog_df.reset_index(drop=True, inplace=True)

    print("Cross-referencing artist catalog with unclaimed works dataset...")
    matches_df = artist_catalog_df[artist_catalog_df['isrc'].isin(unclaimed_isrc_set)]
    print(f"Found {len(matches_df)} matches.")

    print(f"Creating the output file: {OUTPUT_XLSX_PATH}")
    with pd.ExcelWriter(OUTPUT_XLSX_PATH, engine='openpyxl') as writer:
        artist_catalog_df.to_excel(writer, sheet_name='Artist Catalog (All)', index=False)
        matches_df.to_excel(writer, sheet_name='Matches in Unclaimed Dataset', index=False)

    print("Task completed successfully!")


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds.")