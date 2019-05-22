__auther__ = "mihir"
from db_wrapper_ne import db_wrapper_ne
from nltk.tokenize import word_tokenize
from nltk.tag import StanfordNERTagger
import MySQLdb
import pickle
from itertools import groupby
# uncomment the commented part if you have previous named entities stored in pickle file
class named_entity:
    def __init__(self):
        self.tagger = StanfordNERTagger('stanford-ner-2018-10-16/classifiers/english.all.3class.distsim.crf.ser.gz','stanford-ner-2018-10-16/stanford-ner.jar',encoding='utf-8')
        #filename = "filename9500.pickle"
        #with open(filename,'rb') as handle:
         # _dic = pickle.load(handle)
        self.named_entity_count = {}

    def get_named_entity(self, tweet):
        per,loc,org = 0,0,0
        #print(tweet)
        text = tweet[1]
        tokenized_tweet = word_tokenize(text)
        taggs = self.tagger.tag(tokenized_tweet)

        _dic = {}
        _dic["PERSON"] = ""
        _dic["LOCATION"] = ""
        _dic["ORGANIZATION"] = ""

        for tag, chunk in groupby(taggs, lambda x:x[1]):
          ans = " ".join(w for w,t in chunk)
          if tag in _dic:
            _dic[tag] += " "+str(ans.encode('utf-8'))
            #print(ans)
            if str(tag) == "O":
               continue
            if str(tag) == "PERSON":
                per += 1
            elif str(tag) == "LOCATION":
                loc += 1
            elif str(tag) == "ORGANIZATION":
                org += 1
            if str(ans.encode('utf-8')).lower() in self.named_entity_count:
                self.named_entity_count[str(ans.encode('utf-8')).lower()] += 1
            else:
                self.named_entity_count[str(ans.encode('utf-8')).lower()] = 1


        entry = []
        entry.append(tweet[0])
        entry.append(tweet[2])
        entry.append(tweet[1])
        entry.extend([per,loc,org])
        return entry, _dic







if __name__ == '__main__':

    ne = named_entity()
    file_name = "named_entity_count.txt"
    ne_db = db_wrapper_ne()

    count = ne_db.read_tweets()

    print (count)
    cnt = 0
    for i in xrange(0,count):
        tweet = ne_db.get_one()
        entry,_dic= ne.get_named_entity(tweet)
        ne_db.insert_named_entity(entry)
        ne_db.insert_named_entity_words(_dic,entry[0])
        cnt+= 1
        print(cnt)
        if cnt %500 == 0:
          with open('filename{0}.pickle'.format(cnt), 'wb') as handle:
            pickle.dump(ne.named_entity_count, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print("Adding the counts: /n")
    for word in ne.named_entity_count:
        ne_db.insert_named_entity_count([word,ne.named_entity_count[word]])
