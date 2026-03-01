# TMDb Data Fetcher Setup Guide

This guide explains how to fetch movie metadata from The Movie Database (TMDb) API.

## 🎯 Overview

The `fetch_tmdb_data.py` script is a **one-time ETL batch job** that:
- Fetches movie details and credits from TMDb API
- Saves data to parquet format for efficient storage
- Never re-fetches existing data
- Handles rate limiting and errors gracefully
- Can be interrupted and resumed without data loss

## 📋 Prerequisites

### 1. Get TMDb API Key

1. Go to https://www.themoviedb.org/
2. Create a free account (or log in)
3. Navigate to Settings → API
4. Request an API key (choose "Developer")
5. Fill out the simple form
6. Copy your API Key (v3 auth)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install individually:
```bash
pip install pandas requests pyarrow
```

## 🚀 Usage

### Step 1: Set API Key

```bash
export TMDB_API_KEY='your_api_key_here'
```

Or add to your `~/.zshrc` or `~/.bashrc`:
```bash
echo "export TMDB_API_KEY='your_api_key_here'" >> ~/.zshrc
source ~/.zshrc
```

### Step 2: Run the Fetcher

```bash
python fetch_tmdb_data.py
```

The script will:
- Read all movie IDs from `DATA/links.csv`
- Check what's already cached
- Fetch missing data from TMDb
- Save progress every 100 movies
- Display progress and ETA

### Step 3: Inspect Results

```bash
python inspect_tmdb_cache.py
```

This shows:
- How many movies have been fetched
- Data completeness statistics
- Sample movies
- File size

## 📁 Output Structure

```
DATA/
├── links.csv                          # Input: movieId → tmdbId mapping
└── tmdb_cache/
    ├── tmdb_movies.parquet           # Output: All movie data
    └── fetch_progress.json           # Progress tracking
```

## 🎬 Data Schema

The parquet file contains:

**Identifiers:**
- `movieId` - MovieLens ID
- `tmdbId` - TMDb ID

**Basic Info:**
- `title`, `original_title`
- `release_date`, `runtime`
- `overview`, `tagline`, `status`

**Ratings & Popularity:**
- `vote_average`, `vote_count`
- `popularity`

**Financial:**
- `budget`, `revenue`

**Credits:**
- `directors` - List of director names
- `writers` - Top 3 writers
- `producers` - Top 3 producers
- `actors` - Top 10 actors with character names and IDs
- `cast_size`, `crew_size`

**Metadata:**
- `genres` - List of genre names
- `keywords` - List of keywords
- `fetched_at` - Timestamp

## 🔄 Resume Capability

If the script is interrupted:
- Progress is automatically saved every 100 movies
- Re-run the script to resume from where it left off
- Already fetched movies are never re-fetched

## ⚙️ Configuration

Edit constants in `fetch_tmdb_data.py`:

```python
RATE_LIMIT_DELAY = 0.25  # 250ms between requests (4/sec)
MAX_RETRIES = 3          # Retry failed requests
RETRY_DELAY = 2          # Wait 2s before retry
```

## 📊 Expected Performance

- **Rate limit:** 4 requests/second (TMDb free tier: 40/second)
- **Time estimate:** ~6 hours for 87,000 movies
- **File size:** ~50-100 MB (parquet compressed)

## 🐛 Troubleshooting

### "TMDB_API_KEY environment variable not set"
- Make sure you exported the key in your terminal
- Verify: `echo $TMDB_API_KEY`

### Rate limit errors (HTTP 429)
- Script automatically handles this
- If persistent, increase `RATE_LIMIT_DELAY`

### Out of memory
- Script saves every 100 movies
- Safe to interrupt and resume

## 💡 Usage Tips

1. **Run overnight:** Fetching 87K movies takes hours
2. **Check logs:** All activity logged to `tmdb_fetch.log`
3. **Inspect periodically:** Run `inspect_tmdb_cache.py` to check progress
4. **Free tier is enough:** No need for paid TMDb account

## 📚 Next Steps

Once data is cached:

```python
import pandas as pd

# Load cached data
df = pd.read_parquet('DATA/tmdb_cache/tmdb_movies.parquet')

# Explore
print(df.head())
print(df.info())

# Merge with MovieLens ratings
ratings = pd.read_csv('DATA/ratings.csv')
merged = ratings.merge(df, on='movieId')
```

## 🔗 Resources

- TMDb API Docs: https://developers.themoviedb.org/3
- TMDb Terms of Use: https://www.themoviedb.org/terms-of-use
- API Status: https://status.themoviedb.org/
