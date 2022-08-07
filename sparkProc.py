from csv import reader
from pyspark import SparkContext

DEC_ROUND = 3

def main():
    sc = SparkContext(appName="finalProjectTests")
    sc.setLogLevel("ERROR")
    # [0] TweetID, [1] AuthorID, [2] RetweetCount, [3] ReplyCount, [4] LikeCount, [5]QuoteCount
    data = sc.textFile("hdfs://group10-1:54310/projectData/") 
    splitdata = data.mapPartitions(lambda x: reader(x))
    head = splitdata.first()
    newData = splitdata.filter(lambda x: x != head)
    
    retweets = retweetData(newData)
    replies = replyData(newData)
    likes = likeData(newData)
    quotes = quoteData(newData)
    
    printData(retweets, replies, likes, quotes, newData)

# Takes the retweet count, maps it with 1, reduces down to get the number of retweets and how many posts with that amount, then sorts it by the number of retweets
def retweetData (newData):
    return newData.map(lambda x: (int(x[2]), 1)).reduceByKey(lambda a,b : a+b).sortBy(lambda x: x[0]).collect() 

# Takes the reply count, maps it with 1, reduces down to get the number of replies and how many posts with that amount, then sorts it by the number of replies
def replyData (newData):
    return newData.map(lambda x: (int(x[3]), 1)).reduceByKey(lambda a,b : a+b).sortBy(lambda x: x[0]).collect()

# Takes the like count, maps it with 1, reduces down to get the number of likes and how many posts with that amount, then sorts it by the number of likes
def likeData (newData):
    return newData.map(lambda x: (int(x[4]), 1)).reduceByKey(lambda a,b : a+b).sortBy(lambda x: x[0]).collect()

# Takes the quote count, maps it with 1, reduces down to get the number of quotes and how many posts with that amount, then sorts it by the number of quotes
def quoteData (newData):
    return newData.map(lambda x: (int(x[5]), 1)).reduceByKey(lambda a,b : a+b).sortBy(lambda x: x[0]).collect()

# Returns the most popular tweet NOTE Only cares about the first post with this amount 
def getMostRtTwt(twtRetweets, sparkData):
    return sparkData.filter(lambda x: int(x[2]) >= twtRetweets).first()
def getMostRepliedTwt(twtReplies, sparkData):
    return sparkData.filter(lambda x: int(x[3]) >= twtReplies).first()
def getMostLikedTwt(twtLikes, sparkData):
    return sparkData.filter(lambda x: int(x[4]) >= twtLikes).first()  
def getMostQuoteTwt(twtQuotes, sparkData):
    return sparkData.filter(lambda x: int(x[5]) >= twtQuotes).first() 

# Finds the tweet with the highest 
def getMostContra(sparkData):
    highestRatio = sparkData.filter(lambda x: int(x[3]) > int(x[4])).filter(lambda x: int(x[3]) > 10).filter(lambda x: int(x[4]) > 0)
    highestRatio = highestRatio.map(lambda x: (float(x[3])/float(x[4]), (x[0], x[1]))).sortBy(lambda x: x[0]).collect()
    return highestRatio

# Prints out the data to be seen
def printData(retweetData, replyData, likeData, quoteData, sparkData):
    totalTweets = 0

    # Number of 
    totalRetweets = 0
    totalReplies = 0
    totalLikes = 0
    totalQuotes = 0

    # Number of posts with zero of...
    zeroRTs = retweetData[0][1]
    zeroReply = replyData[0][1]
    zeroLikes = likeData[0][1]
    zeroQuotes = quoteData[0][1]

    mostRtTweet = getMostRtTwt(retweetData[-1][0], sparkData)
    mostRepliedTweet = getMostRepliedTwt(replyData[-1][0], sparkData)
    mostLikedTweet = getMostLikedTwt(likeData[-1][0], sparkData)
    mostQuotedTweet = getMostQuoteTwt(quoteData[-1][0], sparkData)

    contra = getMostContra(sparkData)
    mostCon = contra[-1]

    for i in retweetData:
        totalTweets += i[1]
        totalRetweets += i[0] * i[1]
    for i in replyData:
        totalReplies += i[0] * i[1]
    for i in likeData:
        totalLikes += i[0] * i[1]
    for i in quoteData:
        totalQuotes += i[0] * i[1]


    print("Out of {} tweets:".format(totalTweets))
    # RTs
    print("- There are {} total retweets for an average of {} retweets per post".format(totalRetweets, round((float(totalRetweets)/float(totalTweets)), DEC_ROUND)))
    print("- There are {} posts containing 1 or more retweet, giving the average of {} retweets per post".format((totalTweets - zeroRTs), round(float(totalRetweets)/float(totalTweets - zeroRTs), DEC_ROUND)))
    print("- The most retweeted post at {} was post https://twitter.com/{}/status/{}\n".format(retweetData[-1][0], mostRtTweet[1], mostRtTweet[0])) 

    # Reply
    print("- There are {} total replies for an average of {} replies per post".format(totalReplies, round((float(totalReplies)/float(totalTweets)), DEC_ROUND)))
    print("- There are {} posts containing 1 or more reply, giving the average of {} replies per post".format((totalTweets - zeroReply), round(float(totalReplies)/float(totalTweets - zeroReply), DEC_ROUND)))
    print("- The most replied post at {} was post https://twitter.com/{}/status/{}\n".format(replyData[-1][0], mostRepliedTweet[1], mostRepliedTweet[0])) 

    # Likes
    print("- There are {} total likes for an average of {} likes per post".format(totalLikes, round((float(totalLikes)/float(totalTweets)), DEC_ROUND)))
    print("- There are {} posts containing 1 or more like, giving the average of {} likes per post".format((totalTweets - zeroLikes), round(float(totalLikes)/float(totalTweets - zeroLikes), DEC_ROUND)))
    print("- The most liked post at {} was post https://twitter.com/{}/status/{}\n".format(likeData[-1][0], mostLikedTweet[1], mostLikedTweet[0])) # Note, it doesn't matter if the user is wrong, the link will still work if the tweetID is there

    # Quotes
    print("- There are {} total quotes for an average of {} quotes per post".format(totalQuotes, round((float(totalQuotes)/float(totalTweets)), DEC_ROUND)))
    print("- There are {} posts containing 1 or more quote, giving the average of {} quotes per post".format((totalTweets - zeroQuotes), round(float(totalQuotes)/float(totalTweets - zeroQuotes), DEC_ROUND)))
    print("- The most quoted post at {} was post https://twitter.com/{}/status/{}\n".format(quoteData[-1][0], mostQuotedTweet[1], mostQuotedTweet[0])) 

    print("The most contraversal tweet (The tweet had more replies than likes) with a ratio of {} is the https://twitter.com/{}/status/{}\n".format(round(mostCon[0],3), mostCon[1][1], mostCon[1][0]))

if __name__ == "__main__":
    main()
