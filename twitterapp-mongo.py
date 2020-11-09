# This script takes data from the Twitter API, and writes the data to MongoDB

# Import Dependencies
import tweepy
import pandas as pd
import json
import pymongo
from pymongo import MongoClient

###########################################
# Connecting to the API
###########################################

# select the account to use
print("Type in the account handle")
account = input()

# Connect to Twitter using the API Keys and Tokens
ckey='ckey'
csecret='csecret'
atoken='atoken'
asecret='asecret'

# Defining tweepy authentication
auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Streaming Timeline tweets for a specific account
stream = api.user_timeline(account)

###########################################
# Structuring the data
###########################################

# Creating a list of dicts for a specific twitter accounts timeline
list_of_dicts = []
for each_json_tweet in stream:
    list_of_dicts.append(each_json_tweet._json)

# save the tweets as a text file
with open('tweet_json_tweets.txt', 'w') as file:
    file.write(json.dumps(list_of_dicts, indent=4))

# Turning streamed tweets into DataFrame by creating a list of the tweets
# from the text file, long with the column names
my_demo_list = []
with open('tweet_json_tweets.txt', encoding='utf-8') as json_file:
    all_data = json.load(json_file)
    for each_dictionary in all_data:
        tweet_id = each_dictionary['id']
        text = each_dictionary['text']
        favorite_count = each_dictionary['favorite_count']
        retweet_count = each_dictionary['retweet_count']
        created_at = each_dictionary['created_at']
        my_demo_list.append({'tweet_id': str(tweet_id),
                             'text': str(text),
                             'favorite_count': int(favorite_count),
                             'retweet_count': int(retweet_count),
                             'created_at': created_at,
                            })
        tweet_json = pd.DataFrame(my_demo_list, columns =
                                  ['tweet_id', 'text',
                                   'favorite_count', 'retweet_count',
                                   'created_at'])

###########################################
# Writing to MongoDB
###########################################

# Making a Connection with the local MongoClient
client = MongoClient("mongodb://localhost:27017/")
# create a database
db = client["Twitter"]
# create a collection
tweets = db["Tweets"]

# add the dataframe of the tweets to the collection
tweets.insert_many(tweet_json.to_dict("records"))

print("Twitter data successfully saved to MongoDB")
