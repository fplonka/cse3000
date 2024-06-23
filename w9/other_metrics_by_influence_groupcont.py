import pandas as pd
import numpy as np
import pickle


def print_df_info(df, name):
    return
    pd.set_option('display.max_columns', None)
    print(f"DataFrame: {name}")
    print(f"Types: \n{df.dtypes}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Samples: \n{df.head()}")
    print("------------------------------------------------")

# for each author, what fraction of their papers cites a superstar paper. has a 'bin' field based on whether this 
# value is 0.0-0.2, 0.2-0.4, 0.4-0.6, 0.6-0.8, 0.8-0.1. 
author_ss_citation_percentages = pd.read_csv('author_ss_citation_percentages.csv')
print_df_info(author_ss_citation_percentages, "author ss citation percentages")

# values of some metrics for each paper
innovations = pd.read_csv('metrics/innovations.csv')
entropies = pd.read_csv('metrics/entropies_specter.csv')
cit_diversities = pd.read_csv('metrics/cit_diversities_specter.csv')
ref_diversities = pd.read_csv('metrics/ref_diversities_specter.csv')
citation_counts = pd.read_csv('metrics/citation_counts.csv')

print_df_info(innovations, "innovations")
print_df_info(entropies, "entropies")
print_df_info(cit_diversities, "cit_diversities")
print_df_info(ref_diversities, "ref_diversities")
print_df_info(citation_counts, "citation_counts")

# papers dataset, which tells us for each paper its authors.
papers = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson', lines=True)
papers = papers[["corpusid", "authorIds"]]
print_df_info(papers, "papers")

authors = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson', lines=True)

with open('superstar_paper_ids.pickle', 'rb') as handle:
    superstar_paper_ids = pickle.load(handle)
print(superstar_paper_ids) # np array of corpusids of superstar papers

# superstar_corpusids_set = set(superstar_paper_ids)
# innovations['is_superstar'] = innovations['corpusid'].isin(superstar_corpusids_set)
# mean_innovation_superstar = innovations[innovations['is_superstar']]['metric'].mean()
# mean_innovation_non_superstar = innovations[~innovations['is_superstar']]['metric'].mean()
# print("COMPARE:", mean_innovation_non_superstar, mean_innovation_superstar)

def compute_mean_metric_for_bins(metric_df, author_ss_citation_percentages, papers, superstar_corpusids, exclude_superstars=False):
    # Explode the authorIds in the papers dataframe
    papers_exploded = papers.explode('authorIds')

    if exclude_superstars:
        papers_exploded = papers_exploded[~papers_exploded['corpusid'].isin(superstar_corpusids)]

    # 'correct' way i believe: find mean per author, then for each bin take the mean of author means

    # Merge papers with author_ss_citation_percentages on authorIds
    merged_df = papers_exploded.merge(author_ss_citation_percentages, left_on='authorIds', right_on='authorid')
    
    # Merge the resulting dataframe with the metric_df on corpusid
    final_df = merged_df.merge(metric_df, on='corpusid')
    
    # Compute the mean value of the metric for each author
    author_means = final_df.groupby(['authorid', 'bin'])['metric'].mean().reset_index()
    
    # Compute the mean and stderr for each bin based on author means
    bin_means = author_means.groupby('bin')['metric'].agg(['mean', 'count', 'std']).reset_index()
    bin_means['stderr'] = bin_means['std'] / np.sqrt(bin_means['count'])
    
    return bin_means[['bin', 'mean', 'stderr']]

def compute_avg_paper_counts_per_bin(author_ss_citation_percentages, papers, superstar_corpusids, exclude_superstars=False):
    # Explode the authorIds in the papers dataframe
    papers_exploded = papers.explode('authorIds')

    if exclude_superstars:
        papers_exploded = papers_exploded[~papers_exploded['corpusid'].isin(superstar_corpusids)]
    
    # Count the number of papers per author
    paper_counts = papers_exploded.groupby('authorIds').size().reset_index(name='papercount')
    paper_counts.rename(columns={'authorIds': 'authorid'}, inplace=True)
    
    # Merge paper counts with author_ss_citation_percentages
    merged_df = author_ss_citation_percentages.merge(paper_counts, on='authorid')
    
    # Compute the mean paper count for each author in each bin
    author_paper_means = merged_df.groupby(['authorid', 'bin'])['papercount'].mean().reset_index()
    
    # Compute the mean and stderr for each bin based on author means
    bin_paper_means = author_paper_means.groupby('bin')['papercount'].agg(['mean', 'count', 'std']).reset_index()
    bin_paper_means['stderr'] = bin_paper_means['std'] / np.sqrt(bin_paper_means['count'])
    
    return bin_paper_means[['bin', 'mean', 'stderr']]


import matplotlib.pyplot as plt

# Compute the mean metrics for each bin with and without superstars
mean_innovations_per_bin = compute_mean_metric_for_bins(innovations, author_ss_citation_percentages, papers, superstar_paper_ids, False)
mean_innovations_per_bin_no_superstars = compute_mean_metric_for_bins(innovations, author_ss_citation_percentages, papers, superstar_paper_ids, True)

mean_citation_counts_per_bin = compute_mean_metric_for_bins(citation_counts, author_ss_citation_percentages, papers, superstar_paper_ids, False)
mean_citation_counts_per_bin_no_superstars = compute_mean_metric_for_bins(citation_counts, author_ss_citation_percentages, papers, superstar_paper_ids, True)

mean_paper_counts_per_bin = compute_avg_paper_counts_per_bin(author_ss_citation_percentages, papers, superstar_paper_ids, False)
mean_paper_counts_per_bin_no_superstars = compute_avg_paper_counts_per_bin(author_ss_citation_percentages, papers, superstar_paper_ids, True)

idx = 0
def plot_with_error_bars(ax, data_all, data_no_superstars, ylabel, subplot_labels, label_all='All Papers', label_no_superstars='Excluding Superstars Papers', color='orange'):
    ax.plot(data_all['bin'], data_all['mean'], marker='o', label=label_all)
    ax.fill_between(data_all['bin'], 
                    data_all['mean'] - data_all['stderr'], 
                    data_all['mean'] + data_all['stderr'], 
                    alpha=0.2)
    ax.plot(data_no_superstars['bin'], data_no_superstars['mean'], marker='o', label=label_no_superstars, color=color)
    ax.fill_between(data_no_superstars['bin'], 
                    data_no_superstars['mean'] - data_no_superstars['stderr'], 
                    data_no_superstars['mean'] + data_no_superstars['stderr'], 
                    alpha=0.2, color=color)
    ax.set_ylabel(ylabel)

    ax.text(-0.03, 1.03, subplot_labels[plot_with_error_bars.idx], transform=ax.transAxes, 
               fontsize=12, va='center', ha='right')
    plot_with_error_bars.idx += 1
    ax.legend()
    
plot_with_error_bars.idx = 0

# Create subplots
fig, axs = plt.subplots(1, 3, figsize=(15, 5))
subplot_labels = ['(A)', '(B)', '(C)', '(D)', '(E)', '(F)']

# Plot Mean Innovations per Bin with and without superstars
plot_with_error_bars(axs[0], mean_innovations_per_bin, mean_innovations_per_bin_no_superstars, 'Mean Author Innovation', subplot_labels)

# Plot Average Citation Count per Bin
plot_with_error_bars(axs[1], mean_citation_counts_per_bin, mean_citation_counts_per_bin_no_superstars, 'Mean Author Citation Count', subplot_labels)

# Plot Average Paper Count per Bin
plot_with_error_bars(axs[2], mean_paper_counts_per_bin, mean_paper_counts_per_bin_no_superstars, 'Mean Author Publication Count', subplot_labels)

fig.supxlabel("Fraction of Author's Publications that Cites a Superstar", fontsize=12)

# Adjust layout
plt.tight_layout()

plt.savefig('plots/other_metrics_by_influence_group.png')
# Show plot
plt.show()