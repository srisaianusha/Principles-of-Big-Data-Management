

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
ACCESS_TOKEN = "828790535880056834-e3SD8gyIC38tXngwA2ai5o2kUjHbOt4"
ACCESS_SECRET = "RT7h3Ocy1ooWImfg9s9aOs9Iqp3wiBe7ZoYSvIoEzt7jy"
CONSUMER_KEY = "ctBBHyBSxnlWuHPcRNDCYah6D"
CONSUMER_SECRET = "F6AvznXouUzdj1eMZpfbfOuXmGIMd6oe5drp1ObJ7hZRbWcGuA"

class StdOutListener(StreamListener):

    def on_data(self, data):
        print (data)
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['python', 'javascript', 'ruby'])