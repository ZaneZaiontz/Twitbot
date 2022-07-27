import os, tweepy, search
from csv import reader

# 
'''
# TODO:
#   Create dev account and setup tweepy
#   -   Gather data over current trending tweets from the last 24 hours
#   -   Store data in CSV / prepare
#   -   
#   -
'''


# Might be better just to handle the data by
# reading within the __init__ rather than calling
# and storing in main. 
class tpyData:
    def __init__(self, query):
        self.queryTweets = query

    queryTweets = ''
    API_KEY = ''
    API_SECRET_KEY = ''
    BEARER_TOKEN = ''
    ACCESS_TOKEN = ''
    ACCESS_TOKEN_SECRET = ''

    def set_API_KEY(self, API):
        self.API_KEY = API
    def set_API_SECRET_KEY(self, SECRET):
        self.API_SECRET_KEY = SECRET
    def set_BEARER_TOKEN(self, BEARER):
        self.BEARER_TOKEN = BEARER
    def set_ACCESS_TOKEN(self, accessTok):
        self.ACCESS_TOKEN = accessTok
    def set_ACCESS_TOKEN_SECRET(self, accessTok):
        self.ACCESS_TOKEN_SECRET = accessTok
    def set_query_tweets(self, tweets):
        self.queryTweets = tweets

    def get_API_KEY(self):
        return self.API_KEY
    def get_API_SECRET_KEY(self):
        return self.API_SECRET_KEY
    def get_BEARER_TOKEN(self):
        return self.BEARER_TOKEN
    def get_ACCESS_TOKEN(self):
        return self.ACCESS_TOKEN
    def get_ACCESS_TOKEN_SECRET(self):
        return self.ACCESS_TOKEN_SECRET
    def get_query_tweets(self):
        return self.queryTweets

def main():
    daysBack = 1
    searchTerms = "gun -is:retweet"
    clientInfo = tpyData(searchTerms)
    
    f = open("./login/creds.txt", "r")
    clientInfo.set_API_KEY(f.readline().strip('\n'))
    clientInfo.set_API_SECRET_KEY(f.readline().strip('\n'))
    clientInfo.set_BEARER_TOKEN(f.readline().strip('\n'))
    clientInfo.set_ACCESS_TOKEN(f.readline().strip('\n'))
    clientInfo.set_ACCESS_TOKEN_SECRET(f.readline().strip('\n'))
    f.close()

    # Dont run this so we dont go over our limit
    # search.pullTweets(clientInfo, daysBack)
    

if __name__ == "__main__":
    main()
