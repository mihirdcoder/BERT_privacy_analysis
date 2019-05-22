__author__ = 'mihir'
## Using files shared by __Mz__ as reference.
## This files calls twitterAPI to collect tweets. 
## Needs to be in same folder as db_wrapper.py
## Provide your twitterr API keys where blank. 
import json
from TwitterAPI import TwitterAPI, TwitterError
import time
from db_wrapper import db_wrapper
import datetime
import calendar
from multiprocessing.dummy import Pool as ThreadPool
import functools
import traceback
import random
class twitter_collect_data:
    
    
    
    def setup_credential(self):
        """
        :rtype : object
        """
	consumer_key = "77uXutEei6vBxFy85a8BFKaJj"
        consumer_secret = "zNwQsixWug4VTp1DjKudvXtjNUlUyvhJaEVOWAv60RfJwhcex3"
        access_token_key = "602713815-HA81sHCc8KyOz3aJ74yzT8XKTn2UgeTI3cpRTzC6"
        access_token_secret = "VjRvBsVmfEyK3UmIWoA8b6idGABnIBygydWjbf50pAGdf"
        self.api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
        print( 'connection setuped at ')
    

   
   
                
   

    # collects tweets from Twitter
    def get_tweets(self, user_id):
        number_of_messages_in_last_page = 1
        last_id = -1
        
        all_tweets = {}
        exception_counter = 0
	num_cycles =0
	since_id = -1
        message_count = 0;
	error1, error2 = False, False
        print("in get_tweets",user_id, type(user_id))
        while message_count < 3200:
	    
	    
            try:
                if (last_id == -1 ):
                    
                    r = self.api.request('statuses/user_timeline',
                                                 {'user_id':'{0}'.format(user_id), 'counts':'{0}'.format(200)})
                    message_count += 200
                                                  
                else:

                    r = self.api.request('statuses/user_timeline',
                                         {'user_id':'{0}'.format(user_id),
                                          'max_id':str(last_id-1),
                                          'counts': 200})
                    message_count += 200
            except Exception as e:
                print 'Exception "{0}" in get_tweets.'.format(e)
                if exception_counter < self.num_auth_users :
                    exception_counter+=1
                    self.setup_credential()
                else:
                    print 'sleeping'
                    time.sleep(100)
                    exception_counter = 0
	   	error1, error2 = False, False
                continue
	    
           
	    try:
                text = json.loads(r.text)
            except Exception as e:
                print '<<<<<<<<<<<<<<<<<<<< EXCEPTION in get_tweets: (', e, ') >>>>>>>>>>>>>>>>>>>>>>>>>'
		return [last_id , all_tweets]
	    print("Bhai Type Type: ",type(text))
            if isinstance(text , list):       
                
		number_of_messages_in_last_page = len(text)
                
		
		for message in text:
                    #print("type: ",type(message), " id:",message['id'])
		    last_id = message['id']	
                    all_tweets[str(message['id'])] = [str(message['id']),message['text'].encode('utf-8'),str(user_id)]
		    
			
            elif ('error' in text):
                print '0error is "', text['error'], '" in get_tweets.'
                return [last_id,all_tweets]
            elif ('errors' in text and isinstance(text['errors'], list) and 'message' in text['errors'][0] ):
                if (text['errors'][0]['code'] == 64 or text['errors'][0]['code'] == 99
                    or  text['errors'][0]['code'] == 88 or text['errors'][0]['code'] == 89 or text['errors'][0]['code'] == 135
                    or text['errors'][0]['code'] == 226):               
		    error1=True
		    error2=False 
                    print '1error is ',  text['errors'][0]['message'] , ' in get_tweets.  id: ', user_id, ' name: ', username
                    if exception_counter < self.num_auth_users :
                        exception_counter+=1
                        print 'connection is setting up'
                        self.setup_credential()
                        print 'connection setup'
                    else:
                        print 'sleeping'
                        time.sleep(60)
                        exception_counter = 0
                    continue
                elif (text['errors'][0]['code'] == 32):
                    if(num_cycles > 1):
			print ('\nnum_cycles > 2\n')
                        return [last_id,all_tweets]
                    else:
			error2=True
			error1=False
                        print '2error is "', text['errors'][0]['message'], '" with error code (' , text['errors'][0]['code'], ')  \
                            in get_friends.  id: ', user_id, ' name: ', username, ' type: ', type
                        if exception_counter < self.num_auth_users :
                            exception_counter+=1
                            self.setup_credential()
                        else:
                            print 'sleeping'
                            time.sleep(60)
                            exception_counter = 0
                            num_cycles += 1
                        continue
		
                else:
                    print 'Error is "',text['errors'][0]['message'],'" with code: "', text['errors'][0]['code'] ,'" in get_tweets.'
                    return [last_id,all_tweets]

            else:       
                print ('there is an error with this text: "', text , '" in get_tweets.')
                return [last_id,all_tweets]
        return [last_id , all_tweets]



    
    
if __name__ == '__main__':

    ins = twitter_collect_data()
    ins.setup_credential()
    inserted_users_list = []
    
    r = ins.api.request('statuses/user_timeline',
                                                 {'user_id':'@{0}'.format(702243),
                                                  'counts': '{0}'.format(200)})
    ans = json.loads(r.text)
    print(len(ans))
    users_list = []
    
    start = time.time()
    
    db = db_wrapper()
    
    #db.create_messagesEn()
    #get-user_ids 
    users_list = db.retrieve_users()
    #check if you have inserted ay of them before
    already_inserted = db.already_inserted()
    print('already inserted: ',len(already_inserted))    
    print('type users_list: ', type(users_list[0]), ' type al_ins', type(already_inserted[0]))
    need_to_be_inserted = [user for user in users_list if str(user) not in already_inserted]
    print(len(need_to_be_inserted))
    # Start inserting the remaining.
    print 'users_list size: ', len(users_list)
    cnt = 0
    for user_id in need_to_be_inserted:
        print("Getting tweets of user_no ",cnt)
        print(user_id,type(user_id))
        tweets = ins.get_tweets(user_id)
        tweets = tweets[1]
        db.insert_messagesEn(tweets)
    
        print("user_id",str(user_id),"has been inserted in database")
        cnt += 1
    
   
    



