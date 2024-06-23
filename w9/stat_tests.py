import pandas as pd
from scipy import stats
import numpy as np

def print_df_info(df, name):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 400)
    pd.set_option('display.expand_frame_repr', False)
    print(f"DataFrame: {name}")
    print(f"Types: \n{df.dtypes}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Samples: \n{df.head()}")
    print("------------------------------------------------")

# Load metrics data
innovations = pd.read_csv('metrics/innovations.csv')
entropies = pd.read_csv('metrics/entropies.csv')
cit_diversities = pd.read_csv('metrics/cit_diversities.csv')
ref_diversities = pd.read_csv('metrics/ref_diversities.csv')
citation_counts = pd.read_csv('metrics/citation_counts.csv')
pairwise_diversities = pd.read_csv('metrics/pairwise_diversities.csv')  # Per-author metric
pairwise_diversities.rename(columns={"vector": "metric_pairwise_div"}, inplace=True)
pairwise_diversities.rename(columns={"authorIds": "authorid"}, inplace=True)  # Correcting column name for merging


# Display DataFrame info
print_df_info(innovations, "innovations")
print_df_info(entropies, "entropies")
print_df_info(cit_diversities, "cit_diversities")
print_df_info(ref_diversities, "ref_diversities")
print_df_info(citation_counts, "citation_counts")
print_df_info(pairwise_diversities, "pairwise_diversities")  # Display info of the new metric

# Papers dataset, which tells us for each paper its authors.
papers = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson', lines=True)
papers = papers[["corpusid", "authorIds"]]
print_df_info(papers, "papers")

authors = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson', lines=True)
authors = authors[["authorid", "hindex"]]
print_df_info(authors, "authors")

# Merge metrics DataFrames, excluding pairwise_diversities since it's per-author
metrics_df = pd.merge(innovations, entropies, on='corpusid', how='outer', suffixes=('_innov', '_entrop'))
metrics_df = pd.merge(metrics_df, cit_diversities, on='corpusid', how='outer')
metrics_df.rename(columns={'metric': 'metric_cit_div'}, inplace=True)
metrics_df = pd.merge(metrics_df, ref_diversities, on='corpusid', how='outer')
metrics_df.rename(columns={'metric': 'metric_ref_div'}, inplace=True)
metrics_df = pd.merge(metrics_df, citation_counts, on='corpusid', how='outer')
metrics_df.rename(columns={'metric': 'metric_cit_count'}, inplace=True)

# Calculate correlations for paper metrics
metric_list = ['metric_innov', 'metric_entrop', 'metric_cit_div', 'metric_ref_div', 'metric_cit_count']
correlations = metrics_df[metric_list].corr()
print("Correlations between metrics:\n", correlations)
print("------------------------------------------------")

# Explode 'authorIds' so each row contains one authorid per paper
papers_exploded = papers.explode('authorIds')
papers_exploded.rename(columns={'authorIds': 'authorid'}, inplace=True)

# Merge exploded papers with authors
author_papers = pd.merge(papers_exploded, authors, on='authorid', how='left')

# Merge with metrics
author_metrics = pd.merge(author_papers, metrics_df, on='corpusid', how='left')

# Group by authorid and calculate mean metrics
author_mean_metrics = author_metrics.groupby('authorid').agg({m: 'mean' for m in metric_list})
author_mean_metrics['hindex'] = author_metrics.groupby('authorid')['hindex'].first()

# Merge per-author metrics (pairwise_diversities)
author_mean_metrics = pd.merge(author_mean_metrics, pairwise_diversities, on='authorid', how='left')

# Identify superstars
superstars = author_mean_metrics[author_mean_metrics['hindex'] >= 87]
non_superstars = author_mean_metrics[author_mean_metrics['hindex'] < 87]

# Define function for Cohen's d
def cohens_d(group1, group2):
    diff = group1.mean() - group2.mean()
    pooled_std = np.sqrt((group1.std() ** 2 + group2.std() ** 2) / 2)
    return diff / pooled_std

# Calculate mean of superstar means and non-superstar means, perform t-tests, and compute Cohen's d
results = []
for metric in metric_list + ['metric_pairwise_div']:  # Include the pairwise diversity metric
    superstar_mean = superstars[metric].mean()
    non_superstar_mean = non_superstars[metric].mean()
    t_stat, p_value = stats.ttest_ind(superstars[metric].dropna(), non_superstars[metric].dropna(), equal_var=False)
    d_value = cohens_d(superstars[metric], non_superstars[metric])
    
    results.append({
        'Metric': metric,
        'Superstar Mean': superstar_mean,
        'Non-Superstar Mean': non_superstar_mean,
        'T-Statistic': t_stat,
        'P-Value': p_value,
        "Cohens's d": d_value
    })

# Convert results to DataFrame for better visualization
results_df = pd.DataFrame(results)
print(results_df)