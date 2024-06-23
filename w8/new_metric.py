import pandas as pd
import numpy as np
from scipy.spatial.distance import cosine
import pickle


# Assuming the datasets are loaded from JSON files
papers = pd.read_json('../datasets/papers/all_cs_papers_simple.ndjson', lines=True)
# embeddings = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/embeddings_parallel/all_embeddings_simple2.ndjson', lines=True) # SPECTER

# embeddings = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/embeddings_parallel/subset.ndjson', lines=True) # SPECTER
embeddings = pd.read_json('../datasets/topic_embeddings.ndjson', lines=True) # LDA
embeddings.rename(columns={"topic_embedding": "vector"}, inplace=True)

# embeddings = embeddings.rename({'topic_embedding': 'vector'})
# print(embeddings)

print("LOADED")

# Merge embeddings with papers on 'corpusid'
merged_df = pd.merge(papers, embeddings, on='corpusid')

print("merged.")

# Explode the 'authorIds' so each author has a separate row
merged_df = merged_df.explode('authorIds')

with open('/Users/filip/Code/rp/rdataprep/w9/superstar_paper_ids.pickle', 'rb') as handle:
    superstar_paper_ids = pickle.load(handle)
print(superstar_paper_ids) # np array of corpusids of superstar papers

merged_df = merged_df[~merged_df['corpusid'].isin(superstar_paper_ids)]

# Function to calculate the diversity metric for an author's papers
def calculate_diversity(embeddings):
    if len(embeddings) < 2:
        return 0
    num_papers = len(embeddings)
    pairwise_distances = [cosine(embeddings[i], embeddings[j]) for i in range(num_papers) for j in range(i + 1, num_papers)]
    diversity = (2 / (num_papers * (num_papers - 1))) * np.sum(pairwise_distances)
    return diversity

# Group by author and calculate the diversity metric
print("calculating diversity...")
author_diversity = merged_df.groupby('authorIds')['vector'].apply(lambda x: calculate_diversity(np.vstack(x))).reset_index()
print("done.")

# Save the results to a CSV file
author_diversity.to_csv('/Users/filip/Code/rp/rdataprep/w9/metrics/pairwise_diversities_lda_noss.csv', index=False)
print("saved.")