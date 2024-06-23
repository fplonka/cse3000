import pandas as pd

def print_df_info(df, name):
    pd.set_option('display.max_columns', None)
    print(f"DataFrame: {name}")
    print(f"Types: \n{df.dtypes}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Samples: \n{df.head(100)}")
    print("------------------------------------------------")
    
print("Loading DFs...")
    
papers = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson', lines=True)
authors = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson', lines=True)
citations = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/citations/all_citations_simple2.ndjson', lines=True)

print("DFs loaded...\n")

papers = papers[["corpusid", "authorIds", "citationcount"]]
authors = authors[["authorid", "hindex"]]
citations = citations[["citationid", "citedcorpusid", "citingcorpusid"]]

print_df_info(papers, "papers")
print_df_info(authors, "authors")
print_df_info(citations, "citations")

print()
print("Calculating things...")


# Identify the 99.9th percentile h-index threshold
superstar_threshold = authors['hindex'].quantile(0.999)

print("superstar threshold:", superstar_threshold)

# Find all superstars
superstars = authors[authors['hindex'] >= superstar_threshold]

# Find all papers authored by superstars
papers_exploded = papers.explode("authorIds")
superstar_ids = superstars["authorid"].unique()
superstar_paper_ids = papers_exploded[papers_exploded['authorIds'].isin(superstar_ids)]['corpusid'].unique()

# import pickle
# with open('superstar_paper_ids.pickle', 'wb') as handle:
#     pickle.dump(superstar_paper_ids, handle)

# exit(0)

# Find citations where the cited paper is a superstar paper
superstar_citations = citations[citations['citedcorpusid'].isin(superstar_paper_ids)]

# Create a mapping of paper ids to their authors
papers_exploded = papers.explode('authorIds').rename(columns={'authorIds': 'authorid'})

# Find all papers that cite a superstar paper
citing_superstar_papers = superstar_citations['citingcorpusid'].unique()

# Map these citing papers back to their authors
papers_exploded_only_papers_citing_a_superstar = papers_exploded[papers_exploded['corpusid'].isin(citing_superstar_papers)]

# Calculate the fraction of papers for each author that cite a superstar
total_papers_by_author = papers_exploded.groupby('authorid').size()
citing_papers_by_author = papers_exploded_only_papers_citing_a_superstar.groupby('authorid').size()

citing_papers_by_author.to_csv("author_paper_counts.csv")

# Calculate the fraction
author_citation_fraction = (citing_papers_by_author / total_papers_by_author).fillna(0).reset_index()
author_citation_fraction.columns = ['authorid', 'fraction_of_papers_citing_superstar']

print_df_info(author_citation_fraction, "author citation fraction")

# Define the bins
bins = [0.00, 0.2, 0.4, 0.6, 0.8, 0.99]
labels = ['0.0-0.2', '0.2-0.4', '0.4-0.6', '0.6-0.8', '0.8-1.0']

# bins = [0, 0.1, 0.2, 0.3, 0.5, 1.0]
# labels = ['0.0-0.1', '0.1-0.2', '0.2-0.3', '0.3-0.5', '0.5-1.0']

# bins = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
# labels = ['0.05', '0.15', '0.25', '0.35', '0.45', '0.55', '0.65', '0.75', '0.85', '0.95']

# import numpy as np
# num_bins = 6
# bins = np.linspace(0.01, 0.99, num_bins + 1)
# labels = [(bins[i] + bins[i+1]) / 2 for i in range(num_bins)]
# labels = [f'{label:.2f}' for label in labels]
# print("labels:", labels)

# Create a new column 'bin' with the bin labels
author_citation_fraction['bin'] = pd.cut(author_citation_fraction['fraction_of_papers_citing_superstar'], bins=bins, labels=labels, include_lowest=True)

author_citation_fraction.to_csv("author_ss_citation_percentages.csv")