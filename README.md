Run this to get all the data (~13 hrs):
```
python scraper_v2.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output feeds.json
```

Run this to test it:
```
python scraper_v2.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output test_feeds.json  --dry-limit 20
```
