# FDA Social Listening:

This repository is responsible for extracting social data on cannabis derived products from Twitter, Reddit and Instagram. 
To extract data from Instagram, Reddit and Twitter we are using extractors defined inside common git repo.

# Directory Structure:

Below two folder contains relevant keywords and scripts needed to run code successfully- <br>
* data - Folder contain csv file containing all keywords that are needed to extract relevant data <br>
* src - Contains extractor.py, which is the main file responsible for extracting data. This directory also contains<br> 
   the sentiment analyzer file, which is responsible for tagging the social mentions as positive, neutral or negative.<br>

# Important DB tables:

* twitter_mention_raw: contains monthly data extracted from twitter<br>
* reddit_mention_raw: contains monthly data extracted from reddit<br>
* instagram_mention_raw: contains recent data extracted from instagram<br>
* social_data_raw: all the above tables are dumped into social_data_raw to have single source of all extracted data<br>
* social_data_processed: tables contain filtered out data from social_data_raw after doing sentiment analysis.<br>

