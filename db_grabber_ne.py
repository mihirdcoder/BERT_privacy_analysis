author__ = "mihir"

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

class db_grabber_ne:

    NE_table = "NycTweetsNE1"
    read_table = "NycUserDataFinal"
    count_table = "NycNECount"
    word_table = "NycWordNE"
    bert_table = "NycBert1"
    u_db_name = "twitterBert"
    u_host_name = "localhost"
    u_name = "mparulekar"
    u_pass = None
    u_charset = "utf8mb4"
    u_read_default_file = "~/.my.cnf"

    def __init__(self):
        self.mydb_Bert = MyDB(db_grabber_ne.u_host_name, db_grabber_ne.u_db_name, db_grabber_ne.u_name,
                            db_grabber_ne.u_charset, db_grabber_ne.u_read_default_file, db_grabber_ne.u_pass)

        self.mydb_Bert1 = MyDB(db_grabber_ne.u_host_name, db_grabber_ne.u_db_name, db_grabber_ne.u_name,
                            db_grabber_ne.u_charset, db_grabber_ne.u_read_default_file, db_grabber_ne.u_pass)

    def insertBert1(self,vals):
        message_id = vals[4]
        bert11 = "'"+ vals[0]+"'"
        cls11 ="'"+vals[1]+"'"
        bert12 = "'"+vals[2]+"'"
        cls12 = "'"+vals[3]+"'"
        q = "INSERT INTO {0} (message_id,layer12,cls12,layer11,cls11) values(%s,%s,%s,%s,%s)".format(self.bert_table)%(message_id, bert12,cls12,bert11,cls11)
        print(q)
        try:
            self.mydb_Bert1.query(q)
        except Exception as e:
            print ("insert messageEn: ", e)




    def insertBert(self,vals):
        message_id = "'"+vals[4]+"'"
        bert11 = "'"+vals[0]+"'"
        cls11 ="'"+vals[1]+"'"
        bert12 = "'"+vals[2]+"'"
        cls12 = "'"+vals[3]+"'"
        q = "INSERT INTO {0} (message_id,layer11,cls11,layer12,cls12) values(%s,%s,%s,%s,%s)".format(self.bert_table)%(message_id, bert11,cls11,bert12,cls12)
        try:
            self.mydb_Bert1.query(q)
        except Exception as e:
            print ("insert messageEn: ", e)



    def read_tweets(self):

        q = "SELECT message_id, message from {0}".format(self.read_table)
        #print(q)
        try:
           self.mydb_Bert.query(q)
        except Exception as e:
           print ("read_tweets: ", e)
        return self.mydb_Bert.db_cur.rowcount

    def get_50(self):
        try:
            tweet = self.mydb_Bert.db_cur.fetchmany(size = 50)
            print("get_50", len(tweet))
            return tweet
        except Exception as e:
            print ("get_one: ",e)
