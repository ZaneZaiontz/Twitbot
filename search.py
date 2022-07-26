import tweepy as tp
from tweetBot import tpyData

def pullTweets(clientInfo):
    
    client = tp.Client(clientInfo.get_BEARER_TOKEN)
    # print("{}\n{}\n{}\n{}\n{}".format(clientInfo.get_API_KEY(), clientInfo.get_API_SECRET_KEY(), clientInfo.get_BEARER_TOKEN(), clientInfo.get_ACCESS_TOKEN(), clientInfo.get_ACCESS_TOKEN_SECRET()))    
    

    client.search_recent_tweets(query=clientInfo.get_query_tweets())