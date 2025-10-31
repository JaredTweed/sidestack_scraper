## Overview
This repo scrapes a nearly complete list of Substack RSS feeds and corresponding details into a json (which is also saved in this repo).

All the substacks pulled from this are in `feeds.zip` which was last updated on October 30th, 2025. The feeds were scraped from [sidestack.io](https://sidestack.io/).

## Pulling updated data
Run this to get all the data (~13 hrs):
```
python scraper.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output feeds.json
```

Run this to test it:
```
python scraper.py   --sidestack-directory-base https://sidestack.io/directory/all   --max-workers 96    --output test_feeds.json  --dry-limit 20
```

## Parsing
Once `feeds.json` is in the root folder, you can parse it by running any one of the following lines:
```
python map_feeds.py --min-subscribers 1000
python map_feeds.py --min-subscribers 2.5k
python map_feeds.py --min-subscribers 1m
```
and that will create `feeds_mapped.json` which is a more compact version of `feeds.json` excluding non-"OK" status feeds. If you include the `--merge-other-feeds`, then `feeds_mapped.json` will include the feeds from `other_feeds.json` as well which includes feeds not from substack.

To create the suggested feeds for [Feeder](https://github.com/spacecowboy/Feeder), I ran `python map_feeds.py --min-subscribers 1000 --merge-other-feeds`.

## How it works
The code iterates through all of these:
```
https://sidestack.io/directory/all/%230-9
https://sidestack.io/directory/all/A
...
https://sidestack.io/directory/all/Z
```
And then it opens all of these links available: `https://sidestack.io/directory/substack/<substack-slug>`
