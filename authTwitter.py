# Prof. Steven Lindo
# October 2017
# Data Mining Twitter - Twitter Auth example
# Required: pip install twitter
# Go to app.twitter.com to create your own app and generate keys

import twitter


def authTW():
    CONSUMER_KEY = 'KEs0FbtcSxVPiX8VsfTrrP7wD'
    CONSUMER_SECRET = '9tx03UudeS9hY1XwCWycVZXvIRPCdxSVqJ3Iy5WsUtt3BgP9vO'

    OAUTH_TOKEN = '105130797-Ci8OQ1hv9YWyQ86p5ljKsRozxYGXz9NDFmZpTA1l'
    OAUTH_TOKEN_SECRET = 'jc2WqZqSxE43xPaENMyfQR7Ai8EixWTZtER6T7fzxbWly'

    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = twitter.Twitter(auth=auth)

    return twitter_api


def authTWStream():
    CONSUMER_KEY = 'KEs0FbtcSxVPiX8VsfTrrP7wD'
    CONSUMER_SECRET = '9tx03UudeS9hY1XwCWycVZXvIRPCdxSVqJ3Iy5WsUtt3BgP9vO'

    OAUTH_TOKEN = '105130797-Ci8OQ1hv9YWyQ86p5ljKsRozxYGXz9NDFmZpTA1l'
    OAUTH_TOKEN_SECRET = 'jc2WqZqSxE43xPaENMyfQR7Ai8EixWTZtER6T7fzxbWly'

    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
    twitter_stream_api = twitter.TwitterStream(auth=auth)

    return twitter_stream_api


tw_obj = authTW()
print(tw_obj)
