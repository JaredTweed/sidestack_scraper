## Running it
Run this to get all the data (~13 hrs):
```
python scraper_v2.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output feeds.json
```

Run this to test it:
```
python scraper_v2.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output test_feeds.json  --dry-limit 20
```
## How it works
The code iterates through all of these:
```
https://sidestack.io/directory/all/%230-9
https://sidestack.io/directory/all/A
...
https://sidestack.io/directory/all/Z
```
And then it opens all of these links available: `https://sidestack.io/directory/substack/<substack-slug>`
