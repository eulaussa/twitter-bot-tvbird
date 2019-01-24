import tweepy
import time
import datetime
import requests as req
import pandas as pd
import os
from sqlalchemy import create_engine

fetched = False
df = pd.DataFrame()

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)

mention_ids = 'mentionids.txt'
words_tupple1 = ('en çok ne', 'en fazla ne', 'birinci ne', 'birinci kim', 'birinci', 'en fazla kim', 'en çok izlenen',
    'en fazla izlenen', 'en çok kim')
words_tupple2 = ('özetini', 'özet', 'istatitikler', 'istatistiklerini' , 
    'sonuçlarını', 'sonuçlar', 'değerlendirme', 'değerlendirmesi', 'değerlendirmesini')
words_tupple3 = ('en uzun program', 'en uzun', 'uzun', 'süren')

def fetchData():
    address = 'https://www.tv8.com.tr/reyting-sonuclari'
    data = pd.read_html(address,header = None, index_col=None)
    df = (data[0]).drop(columns = ['SIRA', 'TARIH'])
    df.rename(columns={'KANAL ADI':'KANAL', 'BAŞLANGIÇ SAATİ': 'shour', 'BİTİŞ SAATİ': 'ehour'}, inplace=True)
    df.index += 1
    print("New data fetched!")
    return df
  
def uploadData(df):
    now = datetime.datetime.now()
    strnow = now.strftime('%d-%m-%Y')
    engine = create_engine('postgresql://'+POSTGRESQL_USER+':'+POSTGRESQL_PASSWORD+'@'+POSTGRESQL_HOST_IP+':'
                           +POSTGRESQL_PORT+'/'+POSTGRESQL_DATABASE,echo=False)
    df.to_sql(name = strnow, con = engine, if_exists = 'replace', index = True)
    print("Upload to database is successful!")
    return

def tweetRatings(df):
    api.update_status("Eveeet, bugünün reytingleri geliyor, kim ne izlemiş her şey teker teker ortaya çıkıyor!")
    for i in df.index:
        tweet = str(i) + " " + df.at[i, 'BAŞLIK'] + ", Kanal: " + df.at[i, 'KANAL']+ ", Reyting: " + str(df.at[i, 'RTG']) 
            + ", Share: " + str(df.at[i, 'SHARE'])
        api.update_status(tweet)
        time.sleep(5)
    print("New day new tweets")
    return

def storeMentionID(lastMentionID):
    file = open(mention_ids,'a')
    file.write(str(lastMentionID) + "\n")
    file.close()
    return

def findMentionID(mentionID):
    if (os.stat(mention_ids).st_size == 0):
        print("Empty")
        return False
    elif str(mentionID) in open(mention_ids).read():
        return True
    else:
        return False

def beautifyWords(words):
    words = words.lower()
    if 'programlari' in words:
        words = words.replace('programlari','programları')

    if 'guncel' in words:
        words = words.replace('guncel', 'güncel')
    elif 'egitim' in words:
        words = words.replace('egitim', 'eğitim')
    elif 'cocuk' in words:
        words = words.replace('cocuk', 'çocuk')
    elif 'kultur' in words:
        words = words.replace('kultur', 'kültür')
    elif 'eglence' in words:
        words = words.replace('eglence', 'eğlence')
    elif 'gercek' in words:
        words = words.replace('gercek', 'gerçek')
    
    return words

def calculateTimeDif(times):
    tm1 = [x.split(':') for x in times]
    tm1 = [[int(x) for x in k] for k in tm1]
    tm2 = [x[0]*3600+x[1]*60+x[2] for x in tm1]
    return str(datetime.timedelta(seconds = tm2[1]-tm2[0]))
   
def replyTweet(df):
    print("Checking for new tweets")
    mentions = api.mentions_timeline(tweet_mode = 'extended')
    for mention in mentions:
        if findMentionID(mention.id) == False:
            if mention.full_text == '@tvbirdtweets':
                try:
                    storeMentionID(mention.id)
                    print('Empty mention, taking care of it.')
                    api.create_favorite(mention.id)
                    api.update_status('@' + mention.user.screen_name + ' Reytingler için takipte kalın!', mention.id)
                except tweepy.error.TweepError:
                    pass
            elif any(word in mention.full_text.lower() for word in words_tupple1) :
                api.create_favorite(mention.id)
                print("Number one was asked.")
                tweet = ' Dünün birincisi ' + str(df.at[1,'RTG']) + ' reyting ve ' 
                    + str(df.at[1,'SHARE']) + ' share ile ' + df.at[1, 'BAŞLIK'].title()
                    +'! Kendilerini kutlar, Acun\'u daha iyisini yapmaya davet ederiz! Bizi tercih ettiğiniz için teşekkürler.'
                api.update_status('@' + mention.user.screen_name + tweet, mention.id)
                storeMentionID(mention.id)
            elif any(word in mention.full_text.lower() for word in words_tupple2)  :
                api.create_favorite(mention.id)
                print("Overview of the day was asked.")
                df_RTG = df.groupby('TÜR')['RTG'].mean().sort_values(ascending = False)
                df_RTG = df_RTG.reset_index()
                df_SHARE = df.groupby('SHARE')['RTG'].mean().sort_values(ascending = False)
                df_SHARE = df_SHARE.reset_index()

                api.update_status('@' + mention.user.screen_name 
                                  + ' Günlük reyting istatistiklerini istediniz, sonuçlar geliyor.', mention.id)
                time.sleep(5)
                tweet = ' Bugün en yüksek reyting ortalamasıyla izlenen program türü ' 
                    + str(round(df_RTG.at[0, 'RTG'],2)) + ' ortalama ile ' + beautifyWords(df_RTG.at[0, 'TÜR']) 
                    + ' olmuş. Onları takiben ' + str(round(df_RTG.at[1, 'RTG'],2)) +  ' ile '+ beautifyWords(df_RTG.at[1, 'TÜR'])
                    + ' ve ' + str(round(df_RTG.at[2, 'RTG'],2)) + ' ile ' + beautifyWords(df_RTG.at[2, 'TÜR']) + ' geliyor.'
                api.update_status('@' + mention.user.screen_name + tweet, mention.id)
                storeMentionID(mention.id)
            elif any(word in mention.full_text.lower() for word in words_tupple3) :
                api.create_favorite(mention.id)
                time_diffs = []
                print('Longest tv show was asked.')
                for x in df.index:
                    times = [df.at[x,'shour'],df.at[x,'ehour']]
                    time_diffs.append(calculateTimeDif(times))
                result = datetime.datetime.strptime(max(time_diffs), '%H:%M:%S')
                name = df.at[time_diffs.index(max(time_diffs)), 'BAŞLIK'].capitalize()
                api.update_status('@' + mention.user.screen_name + ' Dünün en uzun süren programı ' 
                                  + str(result.hour) + ' saat ' + str(result.minute) + ' dakika ile '+name+ ' olmuş.', mention.id)
                storeMentionID(mention.id)
            else:
                api.create_favorite(mention.id)
                api.update_status('@' + mention.user.screen_name 
                                  + ' Bana günün birincisini ya da dünün özetini sormayı deneyebilirsiniz!', mention.id)
                storeMentionID(mention.id)
    return

while True:
    if (fetched == False):
        df = fetchData()
        fetched = True
    if (datetime.datetime.today().hour == 13 and datetime.datetime.today().minute == 00):
        uploadData(df)
        tweetRatings(df)
    replyTweet(df)
    time.sleep(15)

     
    
