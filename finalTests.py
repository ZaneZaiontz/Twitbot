from csv import reader
from pyspark import SparkContext

def main():
    sc = SparkContext(appName="finalProjectTests")
    sc.setLogLevel("ERROR")

    #setItUp = setItUp.filter(lambda x: )

    data = sc.textFile("hdfs://group10-1:54310/projectData/")

    splitdata = data.mapPartitions(lambda x: reader(x))
    
    retweet = retweetData(splitdata)
    reply = replyData(splitdata)
    like = likeData(splitdata)
    quote = quoteData(splitdata)
    #needs to be corrected will get done afternoon 08/02. Last thing remaining
    #otherwise it works
    avgRetweet = (retweet[0] + retweet[1])/2
  
    print("\nMax Retweet " + str(retweet[0]))
    print("Min Retweet " + str(retweet[1]))
    print("Average Retweet " + str(retweet[2])  + "%")
    
    print("\nMax Reply " + str(reply[0]))
    print("Min Reply " + str(reply[1]))
    print("Average Reply " + str(reply[2])  + "%")
    
    print("\nMax Like " + str(like[0]))
    print("Min Like " + str(like[1]))
    print("Average Like " + str(like[2])  + "%")
    
    print("\nMax quote " + str(quote[0]))
    print("Min quote " + str(quote[1]))
    print("Average quote " + str(quote[2]) + "%")

def retweetData (splitdata):
    head = splitdata.first()
    #splitdata.top(5)
    newData = splitdata.filter(lambda x: x != head)
    #newData.first()
    retweet = newData.map(lambda x: (int(x[2]), 1))
    # retweet.collect()
    retweet = retweet.reduceByKey(lambda a,b : a+b)
    # retweet.collect()
    retweet = retweet.sortBy(lambda x: x[0])
    final = retweet.collect()
    minRe = retweet.collect()[0][0]
    maxRetweet = retweet.collect()[-1][0]    
    temp = 0
    for i in retweet.collect():
        temp += i[0] * i[1]
        #print(i)
    #print("Outside for loop")
    percent = ((float(temp) / 1000.0) * 100)
    #print(str(percent) + "%") 
    avgRetweet = (maxRetweet + minRe)/2
    final = [maxRetweet, minRe, percent]
    return final

def replyData (splitdata):
    head = splitdata.first()
    #splitdata.top(5)
    newData = splitdata.filter(lambda x: x != head)
    #newData.first()
    reply = newData.map(lambda x: (int(x[3]), 1))
    # reply.collect()
    reply = reply.reduceByKey(lambda a,b : a+b)
    # reply.collect()
    reply = reply.sortBy(lambda x: x[0])
    final = reply.collect()
    minRe = reply.collect()[0][0]
    maxReply = reply.collect()[-1][0]
    temp = 0
    for i in reply.collect():
        temp += i[0] * i[1]
        #print(i)
    #print("Outside for loop")
    percent = ((float(temp) / 1000.0) * 100)
    #print(str(percent) + "%") 
    avgReply = (maxReply + minRe)/2
    final = [maxReply, minRe, percent]
    return final

def likeData (splitdata):
    head = splitdata.first()
    #splitdata.top(5)
    newData = splitdata.filter(lambda x: x != head)
    #newData.first()
    like = newData.map(lambda x: (int(x[4]), 1))
    # like.collect()
    like = like.reduceByKey(lambda a,b : a+b)
    # like.collect()
    like = like.sortBy(lambda x: x[0])
    final = like.collect()
    minLike = like.collect()[0][0]
    maxLike = like.collect()[-1][0]
    temp = 0
    for i in like.collect():
        temp += i[0] * i[1]
        #print(i)
    #print("Outside for loop")
    percent = ((float(temp) / 1000.0) * 100)
    #print(str(percent) + "%") 
    final = [maxLike, minLike, percent]
    return final

def quoteData (splitdata):
    head = splitdata.first()
    #splitdata.top(5)
    newData = splitdata.filter(lambda x: x != head)
    #newData.first()
    quote = newData.map(lambda x: (int(x[5]), 1))
    # quote.collect()
    quote = quote.reduceByKey(lambda a,b : a+b)
    # quote.collect()
    quote = quote.sortBy(lambda x: x[0])
    final = quote.collect()
    minQuote = quote.collect()[0][0]
    maxQuote = quote.collect()[-1][0]
    temp = 0
    for i in quote.collect():
        temp += i[0] * i[1]
        #print(i)
    #print("Outside for loop")
    percent = ((float(temp) / 1000.0) * 100)
    # print(str(percent) + "%") 
    final = [maxQuote, minQuote, percent]
    return final


if __name__ == "__main__":
    main()
