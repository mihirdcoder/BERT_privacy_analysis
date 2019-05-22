__author__ = "mihir"

import MySQLdb

class MyDB():
    def __init__(self, u_host, u_db, u_user, u_charset, u_read_default_file, u_pass = None):
        self.db_connection = None
        if(u_pass == None):
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, charset=u_charset, read_default_file=u_read_default_file)
        else:
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, passwd = u_pass, charset=u_charset, read_default_file=u_read_default_file)
        self.db_cur = self.db_connection.cursor()
        self.db_connection.autocommit(True)

    def query(self, query, params = None):
        if(params == None):
            return self.db_cur.execute(query)
        else:
            return self.db_cur.execute(query, params)

    def __del__(self):
        self.db_connection.close()

class db_wrapper_ne:

    NE_table = "NycTweetsNE1"
    read_table = "NycUserDataFinal"
    count_table = "NycNECount"
    word_table = "NycWordNE"
    u_db_name = "twitterBert"
    u_host_name = "localhost"
    u_name = "mparulekar"
    u_pass = None
    u_charset = "utf8mb4"
    u_read_default_file = "~/.my.cnf"

    def __init__(self):
        self.mydb = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)

        self.mydb_update = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)
        self.mydb_insert_count = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)
        self.mydb_insert_word = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)

    def insert_named_entity(self, vals):
        # This is for soft attck algorithm
        message_id = vals[1]
        user_id = vals[0]
        tweet = "'"+vals[2]+"'"
        person = vals[3]
        location = vals[4]
        organization = vals[5]
        q = "INSERT INTO {0} (user_id,message_id,tweet,person,location,organization) values(%s,%s,%s,%s,%s,%s)".format(self.NE_table)% (message_id,user_id, tweet, person,location,organization)
        #print(q)
        try:
           self.mydb_update.query(q)
        except Exception as e:
           print "insert messagesEn: ", e


    def read_tweets(self):
        q = "SELECT * from {0}".format(self.read_table)
        #print(q)
        try:
           self.mydb.query(q)
        except Exception as e:
           print "read_tweets: ", e
        return self.mydb.db_cur.rowcount

    def get_one(self):
        try:
            tweet = self.mydb.db_cur.fetchone()
            #print("in get_one:",tweet)
            return tweet
        except Exception as e:
            print "get_one: ",e

    def insert_named_entity_count(self,vals):
        word ="'"+ vals[0]+"'"
        count = vals[1]
        q = "INSERT INTO {0} (entity,count) values(%s,%s)".format(self.count_table)%(word,count)
        #print(q)
        try:
            self.mydb_insert_count.query(q)
        except Exception as e:
            print "insert messageEn: ", e

    def insert_named_entity_words(self,_dic,message_id):
        person = ""
        location = ""
        organization = ""
        person = "'"+str(_dic["PERSON"]).decode('utf-8')+"'"
        location = "'"+str( _dic["LOCATION"]).decode('utf-8')+"'"
        organization = "'"+ str(_dic["ORGANIZATION"]).decode('utf-8')+"'"
        if person == "":
          person = "NULL"
        if location== "":
          location = "NULL"
        if organization == "":
          organization = "NULL"
        q = "INSERT INTO {0} (message_id,person,location,organization) values(%s,%s,%s,%s)".format(self.word_table)%(message_id,person,location,organization)
        print(q,message_id)
        try:
            self.mydb_insert_word.query(q)
        except Exception as e:
            print "insert messageEn: ", e


    def inserBertEmbeddings(self,vals):
        message_id = vals[4]
        bert11 = vals[0]
        cls11 = vals[1]
        bert12 = vals[2]
        cls12 = vals[3]
        q = "INSERT INTO {0} (message_id,layer11,cls11,layer12,cls12) values(%s,%s)".fomrat(self.bert_table)%(message_id, bert11,cls11,bert12,cls12)
        try:
            self.mydb_insert_count.query(q)
        except Exception as e:
            print "insert messageEn: ", e
