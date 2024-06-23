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
df = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/abstracts_sorted.ndjson', lines=True)

# Text preprocessing function
def preprocess_text(text):
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(text.lower())
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    clean_tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words]
    return clean_tokens

print("loaded dataframe.")

print("columns:", list(df))
print("shape:", df.shape)

# Applying preprocessing
df['processed_abstracts'] = df['abstract'].apply(preprocess_text)

print("preprocessing applied.")

# Create a document-term matrix
vectorizer = CountVectorizer(max_features=1000, max_df=0.95, min_df=2)
dtm = vectorizer.fit_transform(df['processed_abstracts'].apply(lambda x: " ".join(x)))

print("document-term matrix created")

# Fit LDA model
lda = LatentDirichletAllocation(n_components=25, random_state=42, n_jobs=-1)
lda.fit(dtm)

print("LDA complete!")

# Extract top 20 terms for each topic and combine into a set
def get_top_terms(model, feature_names, no_top_words):
    top_terms = set()
    for topic_idx, topic in enumerate(model.components_):
        top_terms.update([feature_names[i] for i in topic.argsort()[:-no_top_words - 1:-1]])
    return top_terms

feature_names = vectorizer.get_feature_names_out()
top_terms = get_top_terms(lda, feature_names, 40)

# Filter abstracts to keep only top terms
def filter_abstract(tokens, top_terms):
    return [token for token in tokens if token in top_terms]

df['filtered_abstracts'] = df['processed_abstracts'].apply(lambda x: filter_abstract(x, top_terms))

print("filtering complete")

# Convert filtered abstracts back to text format and save
df['filtered_abstracts_text'] = df['filtered_abstracts'].apply(lambda x: " ".join(x))

print("columns:", list(df))
print("shape:", df.shape)

filtered_df = df[['corpusid', 'filtered_abstracts_text', 'publicationdate']]

# Save the filtered abstracts to a new NDJSON file
filtered_df.to_json('/Users/filip/Code/rp/rdataprep/datasets/filtered_abstracts.ndjson', orient='records', lines=True)

print("saving complete")