import pandas as pd
import numpy as np
from autocorrect import Speller

import pandas as pd
import re
import string
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer,PorterStemmer,WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer , CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from langdetect import detect
from bertopic import BERTopic

lemmatizer = WordNetLemmatizer()
spell = Speller(lang='en')

english_stopwords=stopwords.words('english')
sno=SnowballStemmer('english')
sample = pd.read_json("C:\Sorpa Test\jobtitle_data\sample.json")

from googletrans import Translator

translator = Translator()

def translate_eng(text):
    result = translator.translate(text, dest="en")
    return result.text

sample["eng_title"] = sample["JobTitle"].apply(translate_eng)

level = 'Vice Director Manager chief  owner  Consultant Associate Intern Junior mid Lead Senior president head VP officer Executive 1 2 3 4 5 6 7 8  i ii iii iv v'

level_to_be_removed = 'intern junior mid lead senior 1 2 3 4 5 6 7 8  i ii iii iv v'.split()


level = level.split()

def remove_seniority(text):
    text = text.lower()
    text = text.replace('intern',"").replace('junior',"").replace('mid',"").replace('lead',"").replace('senior',"")
    text = re.sub('\d|\si*$',"",text)
    return text.strip()


def get_level(title):
    level_word = None
    for word in title.split(' '):
        if word in level:
            level_word = word
            break
    return level_word


specal_chars = [",", "@", "?", ".", " ", "/", "\\", "(", ")", "<", ">", "-", "_", '"', '!', '*', '=', '&']


def preprocess(title):
    #     make title in lower case
    #     remove stop words
    #     Spelling correction
    #     stem them to the root form

    if title is not None:
        nltk_tokens = nltk.word_tokenize(title)

        nltk_tokens = [lemmatizer.lemmatize(spell(word).lower()) for word in nltk_tokens if
                       spell(word).lower() not in english_stopwords
                       and word not in specal_chars]

        final_title = " ".join(nltk_tokens)

        title = final_title

    return title


# lemmatizer.lemmatize()

def stem_words(title):
    #     make title in lower case
    #     remove stop words
    #     Spelling correction
    #     stem them to the root form

    if title is not None:
        nltk_tokens = nltk.word_tokenize(title)

        nltk_tokens = [sno.stem(word) for word in nltk_tokens if word not in english_stopwords]

        final_title = " ".join(nltk_tokens)

        title = final_title

    return title


sample['level']= sample['eng_title'].apply(get_level)
sample['eng_title'] = sample['eng_title'].apply(remove_seniority)
sample['preprocessed_title'] = sample['eng_title'].apply(preprocess)

clean_data = sample[["JobTitle","preprocessed_title","Industry","Department"]]
clean_data['Industry'] = clean_data['Industry'].apply(preprocess)
clean_data['Department'] = clean_data['Department'].apply(preprocess)


clean_data.fillna("")

clean_data.drop_duplicates(subset=['preprocessed_title','Industry','Department'],keep='first', inplace=True)

clean_data.to_csv("clean_data.csv")

topic_model = BERTopic( nr_topics='10',n_gram_range=(1,1))

topics, probs = topic_model.fit_transform(clean_data['preprocessed_title'].values)

print(topic_model.get_topic_info())


df = pd.DataFrame({'topic': topics, 'document': clean_data['preprocessed_title'].values, 'industry': clean_data['Industry'].values, 'department': clean_data['Department'].values})

df.to_csv("topic.csv")