__author__ = "mihir"
# Referred hugging face implementation of BERT extract_features file at huuging face.
# This is minimal code implementation.
# layer 11, layer12 aggregate of BERT embeddings is calculated.
# [CLS] token for layer 11 and layer 12 is also collected.
from db_grabber_ne import db_grabber_ne
import torch
import json
from torch.utils.data import TensorDataset, DataLoader, SequentialSampler
from torch.utils.data.distributed import DistributedSampler
import re
from pytorch_pretrained_bert.tokenization import BertTokenizer
from pytorch_pretrained_bert.modeling import BertModel
import numpy as np
import MySQLdb
import collections

class InputExample(object):

    def __init__(self, unique_id, text_a, text_b):
        self.unique_id = unique_id
        self.text_a = text_a
        self.text_b = text_b

class InputFeatures(object):

    def __init__(self, unique_id, tokens, input_ids, input_mask, input_type_ids):
        self.unique_id = unique_id
        self.tokens = tokens
        self.input_ids = input_ids
        self.input_mask = input_mask
        self.input_type_ids = input_type_ids



# Then this
def convert_examples_to_features(examples, seq_length, tokenizer):
    """Loads a data file into a list of `InputFeature`s."""

    features = []
    for (ex_index, example) in enumerate(examples):
        tokens_a = tokenizer.tokenize(example.text_a)

        tokens_b = None
        if example.text_b:
            tokens_b = tokenizer.tokenize(example.text_b)

        if tokens_b:

            _truncate_seq_pair(tokens_a, tokens_b, seq_length - 3)
        else:
            if len(tokens_a) > seq_length - 2:
                tokens_a = tokens_a[0:(seq_length - 2)]

        tokens = []
        input_type_ids = []
        tokens.append("[CLS]")
        input_type_ids.append(0)
        for token in tokens_a:
            tokens.append(token)
            input_type_ids.append(0)
        tokens.append("[SEP]")
        input_type_ids.append(0)

        if tokens_b:
            for token in tokens_b:
                tokens.append(token)
                input_type_ids.append(1)
            tokens.append("[SEP]")
            input_type_ids.append(1)

        input_ids = tokenizer.convert_tokens_to_ids(tokens)

        input_mask = [1] * len(input_ids)

        while len(input_ids) < seq_length:
            input_ids.append(0)
            input_mask.append(0)
            input_type_ids.append(0)

        assert len(input_ids) == seq_length
        assert len(input_mask) == seq_length
        assert len(input_type_ids) == seq_length

        features.append(
            InputFeatures(
                unique_id=example.unique_id,
                tokens=tokens,
                input_ids=input_ids,
                input_mask=input_mask,
                input_type_ids=input_type_ids))
    return features


def _truncate_seq_pair(tokens_a, tokens_b, max_length):
    while True:
        total_length = len(tokens_a) + len(tokens_b)
        if total_length <= max_length:
            break
        if len(tokens_a) > len(tokens_b):
            tokens_a.pop()
        else:
            tokens_b.pop()
## This will run first
def read_examples(tweets):
    examples = []


    for tweet in tweets:
        unique_id = tweet[0]
        tweet = tweet[1].strip()
        text_a = None
        text_b = None
        m = re.match(r"^(.*) \|\|\| (.*)$", tweet) #does nothing I have just kept it here as orginal script
        if m is None:
            text_a  = tweet
        else:
            text_a = m.group(1)
            text_b = m.group(2)
        examples.append(InputExample(unique_id=unique_id, text_a = text_a, text_b = text_b))

    return examples








def main():
    local_rank = -1
    seq_length = 240
    batch_size = 512
    tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
    model = BertModel.from_pretrained("bert-base-uncased")
    ne_db = db_grabber_ne()


    print("here:")
    #sample you can remove text if you just want to check the implementation then comment line 150 - 159 and check.
    text = [ "Clapper has the perfect name shake his hand get the clap - funny how libs care nothing about American people or WO… https://t.co/M11U8eneB6 " ,
 "NOT Enough @jimmykimmel #AffleckBrothers https://t.co/snD41wy7rE ", "@kayleyhyde A day for gal pals ",
 "RT @TwitterMoments: Disgraced movie producer Harvey Weinstein has surrendered to the New York police over sexual misconduct allegations. ht… ",
 "RT @angelayee: Harvey Weinstein is turning himself in at the police station right across the street from our studios this morning "  ,
 "RT @nyknicks: 25 years. THE DUNK", "In his own words ? https://t.co/2N7Uj6g5uI","Trips to ice cream parlors are the best! @eggers_icecream @PS22si #icecream #yum https://t.co/u52AdYtDri","This story on an orthodontist with $1M in student loan debt makes me really, really uncomfortable https://t.co/WiNYFo6cgc" ]
    print("here")


    count = ne_db.read_tweets()
    print("count = ", count)
    i = 0
    while i <= count:

      text = ne_db.get_50() #ne_db.mydb_Bert.db_cur.fetchmany(50)
      print(i," done","text_len:", len(text))
      i+= 50


      examples = read_examples(text)
      features = convert_examples_to_features(examples, seq_length,tokenizer )

      unique_id_to_feature = {}
      for feature in features:
          unique_id_to_feature[feature.unique_id] = feature

      all_input_ids = torch.tensor([f.input_ids for f in features], dtype=torch.long)
      all_input_mask = torch.tensor([f.input_mask for f in features], dtype=torch.long)
      all_example_index = torch.arange(all_input_ids.size(0), dtype=torch.long)

      eval_data = TensorDataset(all_input_ids, all_input_mask, all_example_index)

      if local_rank == -1:
          eval_sampler = SequentialSampler(eval_data)

      eval_dataloader = DataLoader(eval_data, sampler=eval_sampler, batch_size=batch_size)
      model.eval()
      #print("Len eval_dataloader", len(eval_dataloader))
      for input_ids, input_mask, example_indices in eval_dataloader:
          #print("len input_ids",len(input_ids), "input_mask",len(input_mask),"example_indices",len(example_indices))
          all_encoder_layers, _ = model(input_ids, token_type_ids=None, attention_mask=input_mask)
          layer_output_11 = all_encoder_layers[-2].detach().cpu().numpy()
          layer_output_12 = all_encoder_layers[-1].detach().cpu().numpy()

          #print("len(layer_output)", len(layer_output_11),"len(layer_output[0])", len(layer_output_11[0]), "len(layer_output[0][0])", len(layer_output_11[0][0]), "len(layer_output[0][1])", len(layer_output_11[0][1]) )
          print(" ")
          all_encoder_layers = all_encoder_layers
          cnt = 0
          for b, example_index in enumerate(example_indices):
              feature = features[example_index.item()]
              print(b, "unique_id ", feature.unique_id, "len_feature_tokens:",len(feature.tokens),"feature_token[0] ",feature.tokens[0]  )
              unique_id = int(feature.unique_id)
              all_out_features = []
              needed11 = layer_output_11[example_index][1:len(feature.tokens)]
              needed12 = layer_output_12[example_index][1:len(feature.tokens)]
              #print("needed_len",len(needed11),"len(needed[0])",len(needed12[0]))
                #print(needed)
              _sum11 = needed11.sum(0)
              _sum11 = np.divide(_sum11,len(needed11)-1)
              _sum12 = needed12.sum(0)
              _sum12 = np.divide(_sum12,len(needed12)-1)
              cls11 = layer_output_11[example_index][0]
              cls12 = layer_output_12[example_index][0]
            #print(_sum.shape)
              ##print(layer_output_11[example_index][0].shape, _sum12.shape, type(layer_output_11[example_index][0]), type(_sum11))
              _sum11 = json.dumps(_sum11.tolist())
              _sum12 = json.dumps(_sum12.tolist())
              cls11 = json.dumps(cls11.tolist())
              cls12 = json.dumps(cls12.tolist())
              print(len(_sum11), type(_sum12),type(cls11))
              #output_json = collections.OrderedDict()
              #output_json["layer11"] = _sum11.tolist()
              #_sum11 = json.dumps(output_json)
              ne_db.insertBert1([_sum11,cls11,_sum12,cls12,str(unique_id)])
              print("inserted")
if __name__ == "__main__":
    main()
