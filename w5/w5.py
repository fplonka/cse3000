import polars as pl
import numpy as np

# Load the datasets
papers = pl.read_ndjson("../datasets/papers/all_cs_papers_simple.ndjson")
citations = pl.read_ndjson("../datasets/citations/all_citations_simple2.ndjson")
embeddings = pl.read_ndjson("../datasets/topic_embeddings.ndjson")

# Step 1: Join papers with citations to get the cited papers for each paper
joined_df = citations.join(papers.select(['corpusid']), left_on='citingcorpusid', right_on='corpusid')

# Step 2: Merge the embeddings data with the citation information
citations_with_embeddings = joined_df.join(embeddings, left_on='citedcorpusid', right_on='corpusid')

# Step 3: Group by the citing paper and compute the mean embedding vector for each group
mean_embeddings = citations_with_embeddings.group_by('citingcorpusid').agg([
    pl.col('topic_embedding').map_elements(lambda x: [sum(values) / len(values) for values in zip(*x)], return_dtype=pl.List(pl.Float64)).alias('mean_embedding')
])

# Merge the mean embeddings back with the citations
citations_with_mean_embeddings = citations_with_embeddings.join(mean_embeddings, left_on='citingcorpusid', right_on='citingcorpusid')

# mean_embeddings.write_ndjson("tmp2.ndjson")

# Step 4: Define a function to compute cosine similarity
def cosine_similarity(v1, v2):
    v1 = np.array(v1)
    v2 = np.array(v2)
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

# Step 5: Calculate the desired metric for each paper
def calculate_metric(df):
    mean_embedding = df['mean_embedding'][0]
    embeddings = df['topic_embedding']
    n = len(embeddings)
    cosine_diffs = [(1 - cosine_similarity(embedding, mean_embedding)) for embedding in embeddings]
    metric = sum(cosine_diffs) / n
    if n == 1:
        metric = 0.0
    return pl.DataFrame({'corpusid': [df['citingcorpusid'][0]], 'metric': [metric]})

# Group by citing paper and apply the metric calculation
results = citations_with_mean_embeddings.group_by('citingcorpusid').apply(calculate_metric)

# Display the result
print(results)

results.write_ndjson("../datasets/ref_diversity.ndjson")
