#!/usr/bin/env python3
"""
Quick utility to inspect cached TMDb data.

Usage:
    python inspect_tmdb_cache.py
"""

import pandas as pd
from pathlib import Path
import json

DATA_DIR = Path(__file__).parent / "DATA"
CACHE_DIR = DATA_DIR / "tmdb_cache"
OUTPUT_FILE = CACHE_DIR / "tmdb_movies.parquet"
PROGRESS_FILE = CACHE_DIR / "fetch_progress.json"


def main():
    print("=" * 60)
    print("TMDb Cache Inspector")
    print("=" * 60)
    
    # Check progress file
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
        print(f"\n📊 Progress Info:")
        print(f"   Total fetched: {progress.get('total_fetched', 0)}")
        print(f"   Last updated: {progress.get('last_updated', 'N/A')}")
    else:
        print("\n⚠️  No progress file found")
    
    # Check parquet file
    if OUTPUT_FILE.exists():
        df = pd.read_parquet(OUTPUT_FILE)
        print(f"\n📦 Cached Data:")
        print(f"   Total movies: {len(df)}")
        print(f"   File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} MB")
        
        print(f"\n📋 Columns:")
        for col in df.columns:
            non_null = df[col].notna().sum()
            print(f"   - {col}: {non_null}/{len(df)} ({non_null/len(df)*100:.1f}%)")
        
        print(f"\n🎬 Sample Movies:")
        sample = df.sample(min(5, len(df)))[['movieId', 'title', 'release_date', 'vote_average', 'genres']]
        for _, row in sample.iterrows():
            print(f"   • {row['title']} ({row['release_date']}) - ⭐ {row['vote_average']}")
            print(f"     Genres: {', '.join(row['genres']) if row['genres'] else 'N/A'}")
        
        print(f"\n📊 Statistics:")
        print(f"   Avg runtime: {df['runtime'].mean():.1f} min")
        print(f"   Avg rating: {df['vote_average'].mean():.2f} / 10")
        print(f"   Total movies with budget: {df['budget'].gt(0).sum()}")
        print(f"   Date range: {df['release_date'].min()} to {df['release_date'].max()}")
        
    else:
        print("\n⚠️  No cached data found")
        print(f"   Expected location: {OUTPUT_FILE}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
