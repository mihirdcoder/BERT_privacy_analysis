__author__ = 'mihir'
## Used files from __Mz__ for reference.
## This file is to perform mysql operations for twitter_collect_data.py
import MySQLdb
import datetime
import calendar
import json

class MyDB():
    def __init__(self, u_host, u_db, u_user, u_charset, u_read_default_file, u_pass = None):
        self.db_connection = None
        if(u_pass == None):
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, charset=u_charset, read_default_file=u_read_default_file)
        else:
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, passwd = u_pass, charset=u_charset, read_default_file=u_read_default_file)
        self.db_connection.autocommit(True)
        self.db_cur = self.db_connection.cursor()

    def query(self, query, params = None):
        if(params == None):
            return self.db_cur.execute(query)
        else:
            return self.db_cur.execute(query, params)

    def __del__(self):
        self.db_connection.close()

class db_wrapper:
    
    
    messagesEn_table = "NycUserDataFinal"
    user_table = "NewYorkUsers"
    u_db_name = "twitterBert"
    u_host_name = "localhost"
    u_name = "mparulekar"
    u_pass = None
    u_charset = "utf8mb4"
    u_read_default_file = "~/.my.cnf"

    

    def __init__(self):
        self.mydb = MyDB(db_wrapper.u_host_name, db_wrapper.u_db_name, db_wrapper.u_name, 
                            db_wrapper.u_charset, db_wrapper.u_read_default_file, db_wrapper.u_pass)

    def create_messagesEn(self):
        print("creating table messageEN")
        self.mydb.query("DROP TABLE IF EXISTS {0}".format(self.messagesEn_table))
        self.mydb.query("CREATE TABLE {0} ("
                       "message_id varchar(45) NOT NULL DEFAULT 0,"
                       "message varchar(280),"
                       "user_id varchar(45),"
		       "PRIMARY KEY  (message_id))"
                       .format(self.messagesEn_table))

    def insert_messagesEn(self, all_messages):
        """
        :param all_messages: { user_id : { message_id : [ message_text , created_at ], .... } , ... }
        :rtype: object
        """
        #print 'db:insert_messagesEn'
        for message_id in all_messages:
            message_text = "'"+ all_messages[message_id][1]+"'"
	    message_user = all_messages[message_id][2]
            #print(type(message_id[0]),type(message_id[1]),type(message_id[2]))
  	    #print("Message_id: ",message_id, "message_user: ", message_user,"message_text ", message_text)
	    q = "INSERT INTO {0} (message_id,message,user_id) values(%s,%s,%s)".format(self.messagesEn_table)% (message_id,str(message_text),message_user)
	    print(q)
	    try:
               self.mydb.query(q)
            except Exception as e:
               print "insert messagesEn: ", e
      	    		

    def already_inserted(self):
        print 'already visited'
        users = []
        try:
            sql = "select DISTINCT user_id from {0}".format(self.messagesEn_table)
            self.mydb.query(sql)
            result = self.mydb.db_cur.fetchall()
            for row in result:
                user = row[0]
                users.append(user)
        except:
            print "retrieve_users: (Error: unable to fecth data)"
        return users

    def retrieve_users(self, feature='counter', min_value='0'):
        print 'db:retrieve_users'
        users = []
        try:
            sql = "SELECT * FROM {0}".format(self.user_table)
            self.mydb.query(sql)
            result = self.mydb.db_cur.fetchall()
            for row in result:
                user = row[0]
                users.append(user)
        except:
            print "retrieve_users: (Error: unable to fecth data)"        
        #for user in users:
        #    print "user: ",user[0],user[1]
        return users


