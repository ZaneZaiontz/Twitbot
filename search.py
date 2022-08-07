import tweepy as tp
import csv
from datetime import datetime, timedelta

TWT_LIMIT = 1000

def pullTweets(clientInfo, prevDays):
    client = tp.Client(bearer_token=clientInfo.get_BEARER_TOKEN())   

    # [attachments,author_id,context_annotations,conversation_id,created_at,
    #  entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,
    #  possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,
    #  reply_settings,source,text,withheld]
    # Things to add to dataset: 
    # - Time of tweet creation
    # - Source of where tweet came from

    fileName = "dataset2"
    with open('data/%s.csv' % (fileName), 'w', encoding="ascii", errors="ignore") as file:
        w = csv.writer(file)
        w.writerow(['tweet_id', 
                    'author_id', 
                    # 'text', 
                    'retweet_count', 
                    'reply_count', 
                    'like_count', 
                    'quote_count'])
        try:
            for tweet in tp.Paginator(  client.search_recent_tweets, 
                                        query=clientInfo.get_query_tweets(), 
                                        max_results=100, 
                                        tweet_fields=["public_metrics", "source", "author_id"]).flatten(limit=TWT_LIMIT):
                w.writerow([tweet.id, 
                            tweet.author_id, 
                            # tweet.text.encode("ascii"), 
                            tweet.public_metrics['retweet_count'], 
                            tweet.public_metrics['reply_count'], 
                            tweet.public_metrics['like_count'], 
                            tweet.public_metrics['quote_count']])
        except tp.RateLimitError as exc:
            print('Rate limit hit!!!')


if __name__ == "__main__":
    print("no")