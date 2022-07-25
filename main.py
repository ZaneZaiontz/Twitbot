import os, tweepy
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
    # def __init__(self) -> None:
    API_KEY = ""
    API_SECRET_KEY = ""
    BEARER_TOKEN = ""
        
    def set_API_KEY(self, API):
        self.API_KEY = API
    def set_API_SECRET_KEY(self, SECRET):
        self.API_SECRET_KEY = SECRET
    def set_BEARER_TOKEN(self, BEARER):
        self.BEARER_TOKEN = BEARER

    def get_API_KEY(self):
        return str(self.API_KEY)
    def get_API_SECRET_KEY(self):
        return str(self.API_SECRET_KEY)
    def get_BEARER_TOKEN(self):
        return str(self.BEARER_TOKEN)





def main():

    botMain = tpyData()

    f = open("./login/creds.txt", "r")
    botMain.set_API_KEY(f.readline().strip())
    botMain.set_API_SECRET_KEY(f.readline().strip())
    botMain.set_BEARER_TOKEN(f.readline().strip())
    f.close()

    print("{}\n{}\n{}".format(botMain.get_API_KEY(), botMain.get_API_SECRET_KEY(), botMain.get_BEARER_TOKEN()))
    



if __name__ == "__main__":
    main()
