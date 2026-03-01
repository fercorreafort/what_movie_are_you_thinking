#!/usr/bin/env python3
"""
TMDb Data Fetcher - One-time ETL Batch Job

Fetches movie details and credits from The Movie Database (TMDb) API.
Features:
- Fetch once, cache forever
- Resume capability (never re-fetch existing data)
- Rate limiting and error handling
- Saves to parquet for efficient storage
- Progress tracking

Usage:
    export TMDB_API_KEY='your_api_key_here'
    python fetch_tmdb_data.py
"""

import os
import sys
import time
import json
import shutil
import pandas as pd
import requests
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tmdb_fetch.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://api.themoviedb.org/3"
RATE_LIMIT_DELAY = 0.25  # 4 requests per second = 250ms delay
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# File paths
DATA_DIR = Path(__file__).parent / "DATA"
CACHE_DIR = DATA_DIR / "tmdb_cache"
LINKS_FILE = DATA_DIR / "links.csv"
OUTPUT_FILE = CACHE_DIR / "tmdb_movies.parquet"
PROGRESS_FILE = CACHE_DIR / "fetch_progress.json"


class TMDbFetcher:
    """Handles fetching and caching of TMDb data."""
    
    def __init__(self, api_key: str):
        """Initialize the fetcher with API credentials."""
        self.api_key = api_key
        self.session = requests.Session()
        
        # Create cache directory if it doesn't exist
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Load progress state
        self.fetched_ids = self._load_progress()
        
    def _load_progress(self) -> set:
        """Load previously fetched movie IDs."""
        if PROGRESS_FILE.exists():
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    data = json.load(f)
                fetched_ids = data.get('fetched_ids', [])
                return {int(movie_id) for movie_id in fetched_ids}
            except (json.JSONDecodeError, OSError, TypeError, ValueError) as error:
                backup_file = PROGRESS_FILE.with_suffix('.json.corrupt')
                try:
                    shutil.copy2(PROGRESS_FILE, backup_file)
                    logger.error(
                        f"Corrupted progress file detected at {PROGRESS_FILE}. "
                        f"Backed up to {backup_file}. Starting with empty progress."
                    )
                except OSError:
                    logger.error(
                        f"Corrupted progress file detected at {PROGRESS_FILE}. "
                        "Could not create backup. Starting with empty progress."
                    )
                logger.debug(f"Progress load error: {error}")
                return set()
        return set()
    
    def _save_progress(self):
        """Save current progress."""
        temp_progress_file = PROGRESS_FILE.with_suffix('.json.tmp')
        payload = {
            'fetched_ids': [int(movie_id) for movie_id in self.fetched_ids],
            'last_updated': datetime.now().isoformat(),
            'total_fetched': len(self.fetched_ids)
        }
        with open(temp_progress_file, 'w') as f:
            json.dump(payload, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_progress_file, PROGRESS_FILE)
    
    def _make_request(self, endpoint: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
        """Make API request with retry logic."""
        url = f"{BASE_URL}{endpoint}"
        
        # Support both API key (v3) and Bearer token (v4) authentication
        # If key looks like a JWT token, use Bearer auth, otherwise use api_key param
        if self.api_key.startswith('eyJ'):
            # Bearer token (Read Access Token)
            headers = {"Authorization": f"Bearer {self.api_key}"}
            params = {}
        else:
            # API Key (v3)
            headers = {}
            params = {"api_key": self.api_key}
        
        for attempt in range(retries):
            try:
                time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
                response = self.session.get(url, params=params, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    logger.error(f"Authentication failed (401). Please check your TMDB_API_KEY.")
                    logger.error(f"Key format detected: {'Bearer Token' if self.api_key.startswith('eyJ') else 'API Key v3'}")
                    logger.error(f"Key prefix: {self.api_key[:10]}...")
                    logger.error(f"Get your API key from: https://www.themoviedb.org/settings/api")
                    logger.error(f"Make sure you're using the 'API Key (v3 auth)' not the 'API Read Access Token'")
                    return None
                elif response.status_code == 404:
                    # Don't spam logs with 404s - some movies don't exist
                    return None
                elif response.status_code == 429:
                    # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limit hit. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"HTTP {response.status_code}: {endpoint}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY)
                    continue
        
        return None
    
    def fetch_movie_details(self, tmdb_id: int) -> Optional[Dict]:
        """Fetch movie details including credits."""
        tmdb_id = int(tmdb_id)

        # Skip if already fetched
        if tmdb_id in self.fetched_ids:
            return None
        
        endpoint = f"/movie/{tmdb_id}?append_to_response=credits,keywords"
        data = self._make_request(endpoint)
        
        if data:
            # Mark as fetched
            self.fetched_ids.add(tmdb_id)
            return data
        
        return None
    
    def extract_movie_data(self, movie_data: Dict, tmdb_id: int, movie_id: int) -> Dict:
        """Extract and flatten relevant movie information."""
        credits = movie_data.get('credits', {})
        
        # Extract cast (top 10)
        cast = credits.get('cast', [])[:10]
        actors = [
            {
                'actor_id': c.get('id'),
                'actor_name': c.get('name'),
                'character': c.get('character'),
                'order': c.get('order')
            }
            for c in cast
        ]
        
        # Extract crew (directors, writers, producers)
        crew = credits.get('crew', [])
        directors = [c.get('name') for c in crew if c.get('job') == 'Director']
        writers = [c.get('name') for c in crew if c.get('department') == 'Writing'][:3]
        producers = [c.get('name') for c in crew if c.get('job') == 'Producer'][:3]
        
        # Extract genres
        genres = [g.get('name') for g in movie_data.get('genres', [])]
        
        # Extract keywords
        keywords = [k.get('name') for k in movie_data.get('keywords', {}).get('keywords', [])]
        
        # Flatten to single record
        return {
            'movieId': movie_id,
            'tmdbId': tmdb_id,
            'title': movie_data.get('title'),
            'original_title': movie_data.get('original_title'),
            'release_date': movie_data.get('release_date'),
            'runtime': movie_data.get('runtime'),
            'budget': movie_data.get('budget'),
            'revenue': movie_data.get('revenue'),
            'vote_average': movie_data.get('vote_average'),
            'vote_count': movie_data.get('vote_count'),
            'popularity': movie_data.get('popularity'),
            'overview': movie_data.get('overview'),
            'tagline': movie_data.get('tagline'),
            'status': movie_data.get('status'),
            'genres': genres,
            'keywords': keywords,
            'directors': directors,
            'writers': writers,
            'producers': producers,
            'actors': actors,
            'cast_size': len(cast),
            'crew_size': len(crew),
            'fetched_at': datetime.now().isoformat()
        }


def load_links() -> pd.DataFrame:
    """Load the links.csv file."""
    logger.info(f"Loading links from {LINKS_FILE}")
    df = pd.read_csv(LINKS_FILE)
    
    # Filter out rows with missing tmdbId
    initial_count = len(df)
    df = df.dropna(subset=['tmdbId'])
    df['tmdbId'] = df['tmdbId'].astype(int)
    
    logger.info(f"Loaded {len(df)} movies with tmdbId (filtered {initial_count - len(df)} missing)")
    return df


def load_existing_data() -> Optional[pd.DataFrame]:
    """Load existing cached data if it exists."""
    if OUTPUT_FILE.exists():
        logger.info(f"Loading existing cache from {OUTPUT_FILE}")
        return pd.read_parquet(OUTPUT_FILE)
    return None


def save_data(df: pd.DataFrame):
    """Save data to parquet file."""
    logger.info(f"Saving {len(df)} records to {OUTPUT_FILE}")
    df.to_parquet(OUTPUT_FILE, index=False, engine='pyarrow')


def main():
    """Main ETL pipeline."""
    # Check for API key
    api_key = os.environ.get('TMDB_API_KEY')
    if not api_key:
        logger.error("TMDB_API_KEY environment variable not set!")
        logger.error("Get your API key from: https://www.themoviedb.org/settings/api")
        logger.error("Then run: export TMDB_API_KEY='your_key_here'")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("TMDb Data Fetcher - One-time ETL Batch Job")
    logger.info("=" * 60)
    
    # Load links
    links_df = load_links()
    total_movies = len(links_df)
    
    # Initialize fetcher
    fetcher = TMDbFetcher(api_key)
    
    # Load existing data
    existing_df = load_existing_data()
    if existing_df is not None:
        logger.info(f"Found {len(existing_df)} existing records")
    
    # Determine which movies to fetch
    movies_to_fetch = links_df[~links_df['tmdbId'].isin(fetcher.fetched_ids)]
    logger.info(f"Need to fetch: {len(movies_to_fetch)} movies")
    
    if len(movies_to_fetch) == 0:
        logger.info("✓ All movies already fetched! Nothing to do.")
        return
    
    # Fetch movies
    new_records = []
    logger.info("Starting fetch...")

    progress_bar = tqdm(
        movies_to_fetch.itertuples(index=False),
        total=len(movies_to_fetch),
        desc="Fetching TMDb",
        unit="movie",
    )

    for idx, row in enumerate(progress_bar, 1):
        movie_id = int(row.movieId)
        tmdb_id = int(row.tmdbId)
        progress_bar.set_postfix_str(f"movieId={movie_id} tmdbId={tmdb_id}")
        
        # Fetch data
        movie_data = fetcher.fetch_movie_details(tmdb_id)
        
        if movie_data:
            # Extract and store
            record = fetcher.extract_movie_data(movie_data, tmdb_id, movie_id)
            new_records.append(record)
            title = movie_data.get('title') or movie_data.get('original_title') or 'Unknown title'
            progress_bar.set_postfix_str(
                f"movieId={movie_id} tmdbId={tmdb_id} title={str(title)[:40]}"
            )
        
        # Progress update every 100 movies
        if idx % 100 == 0:
            logger.info(f"Progress: {idx}/{len(movies_to_fetch)} ({idx/len(movies_to_fetch)*100:.1f}%)")
            
            # Save checkpoint
            if new_records:
                checkpoint_df = pd.DataFrame(new_records)
                if existing_df is not None:
                    checkpoint_df = pd.concat([existing_df, checkpoint_df], ignore_index=True)
                save_data(checkpoint_df)
                fetcher._save_progress()
                logger.info(f"Checkpoint saved: {len(checkpoint_df)} total records")
    
    # Final save
    logger.info("Finalizing...")
    if new_records:
        new_df = pd.DataFrame(new_records)
        if existing_df is not None:
            final_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        
        save_data(final_df)
        fetcher._save_progress()
        
        logger.info("=" * 60)
        logger.info(f"✓ ETL Complete!")
        logger.info(f"  New records fetched: {len(new_records)}")
        logger.info(f"  Total records: {len(final_df)}")
        logger.info(f"  Output file: {OUTPUT_FILE}")
        logger.info(f"  Coverage: {len(final_df)}/{total_movies} ({len(final_df)/total_movies*100:.1f}%)")
        logger.info("=" * 60)
    else:
        logger.info("No new records to save")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user. Progress has been saved.")
        logger.info("Run the script again to resume.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
