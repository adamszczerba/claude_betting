# API between modules, change require direct approval. Agent trust in that file until something clearly fails.

scrapers are publishing odds to redis stream,
some signal providers are subscribing to that stream produce signals based on it
csv writer is subscribing to that stream and saves match data to csv files
dashboard is subscribing to that stream, reads logs and presents them