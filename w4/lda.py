import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer
import nltk

# Download NLTK resources
nltk.download('stopwords')
nltk.download('wordnet')

# Load your dataset
# Assuming you have a CSV with an 'abstract' column
# df = pd.read_csv('semantic_scholar_abstracts.csv')
df = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/abstracts_parallel/all_abstracts_simple_dedup.ndjson', lines=True)

# Text preprocessing function
def preprocess_text(text):
    # Tokenization
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(text.lower())

    # Remove stopwords and lemmatize
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    clean_tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words]
    return " ".join(clean_tokens)

print("loaded dataframe.")

# Applying preprocessing
df['processed_abstracts'] = df['abstract'].apply(preprocess_text)

print("preprocessing applied.")

# Create a document-term matrix
vectorizer = CountVectorizer(max_features=1000, max_df=0.95, min_df=2)
dtm = vectorizer.fit_transform(df['processed_abstracts'])

print("document-term matrix created")

# Fit LDA model
lda = LatentDirichletAllocation(n_components=25, random_state=42, n_jobs=-1)
lda.fit(dtm)

print("LDA complete!")

def display_topics(model, feature_names, no_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print(f"Topic {topic_idx}:")
        print(" ".join([feature_names[i] for i in topic.argsort()[:-no_top_words - 1:-1]]))
    
def display_all_topics(model, feature_names):
    for topic_idx, topic in enumerate(model.components_):
        print(f"Topic {topic_idx}:")
        print(" ".join([feature_names[i] for i in topic.argsort()[::-1]]))

feature_names = vectorizer.get_feature_names_out()

# display_topics(lda, feature_names, 10)
display_all_topics(lda, feature_names)


print("saving...")

topic_vectors = lda.transform(dtm)

# Combine topic vectors into a single column
df['topic_embedding'] = topic_vectors.tolist()

# Select only relevant columns (abstract and topic embedding)
ndjson_df = df[['corpusid', 'topic_embedding']]

# Save to NDJSON file
# ndjson_df.to_json('/Users/filip/Code/rp/rdataprep/datasets/topic_embeddings.ndjson', orient='records', lines=True)

# Topic 0:
# device information method unit first second user invention display mobile
# Topic 1:
# resource cloud computing attack application device energy service user security
# Topic 2:
# software test process development system game testing project tool agent
# Topic 3:
# security model key protocol scheme event based simulation authentication analysis
# Topic 4:
# network node traffic wireless scheme protocol performance routing channel packet
# Topic 5:
# method based set algorithm result rule clustering approach proposed pattern
# Topic 6:
# technology computer application research development digital new library paper information
# Topic 7:
# user interaction human interface virtual visual design environment 3d interactive
# Topic 8:
# module communication channel control information base data station system terminal
# Topic 9:
# system control based time controller real paper design performance using
# Topic 10:
# video graph frame temporal cache sequence view kernel representation action
# Topic 11:
# user information web database query content search social data based
# Topic 12:
# service model based application process design approach framework domain paper
# Topic 13:
# method proposed detection based algorithm result estimation image noise using
# Topic 14:
# data technique large analysis time mining processing set application big
# Topic 15:
# feature recognition image object classification face method detection using based
# Topic 16:
# server network file vehicle client storage management request peer connection
# Topic 17:
# time error rate problem number algorithm scheduling result delay probability
# Topic 18:
# learning study student research knowledge question result based evaluation online
# Topic 19:
# signal power design performance frequency circuit architecture high system speech
# Topic 20:
# language code program text word programming document source translation type
# Topic 21:
# model network learning neural training method task performance prediction deep
# Topic 22:
# image memory block coding compression color bit quality processing resolution
# Topic 23:
# algorithm method function proposed optimization problem based paper result new
# Topic 24:
# problem robot algorithm task dynamic approach agent policy path environment
# saving...