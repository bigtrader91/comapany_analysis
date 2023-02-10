import os

import tweepy
from kafka import KafkaProducer
from dotenv import load_dotenv
bearer_token = os.getenv("bearer_token")
producer = KafkaProducer(bootstrap_servers='localhost')

# tweepy.StreamClient 클래스를 상속받는 클래스
class TwitterStream(tweepy.StreamingClient):

    def on_data(self, raw_data):
        producer.send('company', raw_data)
        print(raw_data.decode('utf-8'))

# 규칙 제거 함수
def delete_all_rules(rules):
    # 규칙 값이 없는 경우 None 으로 들어온다.
    if rules is None or rules.data is None:
        return None
    stream_rules = rules.data
    ids = list(map(lambda rule: rule.id, stream_rules))
    client.delete_rules(ids=ids)
    
    
#sample code : https://github.com/twitterdev/Twitter-API-v2-sample-code
#rules : https://docs.tweepy.org/en/stable/streamrule.html#tweepy.StreamRule
#query : https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query

client = TwitterStream( bearer_token = bearer_token)
rules = client.get_rules()
delete_all_rules(rules)

client.add_rules(tweepy.StreamRule(value="삼성전자"))
client.filter(tweet_fields=["lang", "created_at"])
    

    
