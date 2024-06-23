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
entropies_lda = pd.read_csv('metrics/entropies_lda.csv')
cit_diversities_lda = pd.read_csv('metrics/cit_diversities_lda.csv')
ref_diversities_lda = pd.read_csv('metrics/ref_diversities_lda.csv')
pair_diversities_lda = pd.read_csv('metrics/pairwise_diversities_lda.csv')
pair_diversities_lda_noss = pd.read_csv('metrics/pairwise_diversities_lda_noss.csv') # no superstars, cursed but has to be this way.......

entropies_specter = pd.read_csv('metrics/entropies_specter.csv')
cit_diversities_specter = pd.read_csv('metrics/cit_diversities_specter.csv')
ref_diversities_specter = pd.read_csv('metrics/ref_diversities_specter.csv')
pair_diversities_specter = pd.read_csv('metrics/pairwise_diversities_specter.csv')
pair_diversities_specter_noss = pd.read_csv('metrics/pairwise_diversities_specter_noss.csv')

# print_df_info(cit_diversities_lda, "cit_diversities")
# print_df_info(ref_diversities, "ref_diversities")
# print_df_info(citation_counts, "citation_counts")

# papers dataset, which tells us for each paper its authors.
papers = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson', lines=True)
papers = papers[["corpusid", "authorIds"]]
print_df_info(papers, "papers")

authors = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson', lines=True)

with open('superstar_paper_ids.pickle', 'rb') as handle:
    superstar_paper_ids = pickle.load(handle)
print(superstar_paper_ids) # np array of corpusids of superstar papers

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

def compute_mean_metric_for_bins_direct(metric_df, author_ss_citation_percentages):
    # Merge the metric_df with author_ss_citation_percentages on authorid
    merged_df = metric_df.merge(author_ss_citation_percentages, on='authorid')

    # Compute the mean and stderr for each bin based on author metrics
    bin_means = merged_df.groupby('bin')['metric'].agg(['mean', 'count', 'std']).reset_index()
    bin_means['stderr'] = bin_means['std'] / np.sqrt(bin_means['count'])
    
    return bin_means[['bin', 'mean', 'stderr']]

import matplotlib.pyplot as plt


# Compute the mean metrics for each bin with and without superstars
entropy_lda_means = compute_mean_metric_for_bins(entropies_lda, author_ss_citation_percentages, papers, superstar_paper_ids, False)
entropy_lda_means_ns = compute_mean_metric_for_bins(entropies_lda, author_ss_citation_percentages, papers, superstar_paper_ids, True)
entropy_specter_means = compute_mean_metric_for_bins(entropies_specter, author_ss_citation_percentages, papers, superstar_paper_ids, False)
entropy_specter_means_ns = compute_mean_metric_for_bins(entropies_specter, author_ss_citation_percentages, papers, superstar_paper_ids, True)

cit_diversity_lda_means = compute_mean_metric_for_bins(cit_diversities_lda, author_ss_citation_percentages, papers, superstar_paper_ids, False)
cit_diversity_lda_means_ns = compute_mean_metric_for_bins(cit_diversities_lda, author_ss_citation_percentages, papers, superstar_paper_ids, True)
cit_diversity_specter_means = compute_mean_metric_for_bins(cit_diversities_specter, author_ss_citation_percentages, papers, superstar_paper_ids, False)
cit_diversity_specter_means_ns = compute_mean_metric_for_bins(cit_diversities_specter, author_ss_citation_percentages, papers, superstar_paper_ids, True)

ref_diversity_lda_means = compute_mean_metric_for_bins(ref_diversities_lda, author_ss_citation_percentages, papers, superstar_paper_ids, False)
ref_diversity_lda_means_ns = compute_mean_metric_for_bins(ref_diversities_lda, author_ss_citation_percentages, papers, superstar_paper_ids, True)
ref_diversity_specter_means = compute_mean_metric_for_bins(ref_diversities_specter, author_ss_citation_percentages, papers, superstar_paper_ids, False)
ref_diversity_specter_means_ns = compute_mean_metric_for_bins(ref_diversities_specter, author_ss_citation_percentages, papers, superstar_paper_ids, True)

pair_diversity_lda_means = compute_mean_metric_for_bins_direct(pair_diversities_lda, author_ss_citation_percentages)
pair_diversity_lda_means_ns = compute_mean_metric_for_bins_direct(pair_diversities_lda_noss, author_ss_citation_percentages)
pair_diversity_specter_means = compute_mean_metric_for_bins_direct(pair_diversities_specter, author_ss_citation_percentages)
pair_diversity_specter_means_ns = compute_mean_metric_for_bins_direct(pair_diversities_specter_noss, author_ss_citation_percentages)

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

    ax.text(-0.02, 1.02, subplot_labels[plot_with_error_bars.idx], transform=ax.transAxes, 
               fontsize=12, va='center', ha='right')
    plot_with_error_bars.idx += 1
    ax.legend()
    
plot_with_error_bars.idx = 0

# Create subplots
fig, axs = plt.subplots(2, 4, figsize=(20, 12))
subplot_labels = ['(A)', '(B)', '(C)', '(D)', '(E)', '(F)', '(G)', '(H)']

# Plot Mean Reference Diversities per Bin with and without superstars
plot_with_error_bars(axs[0, 0], entropy_lda_means, entropy_lda_means_ns, 'Mean Author Entropy (LDA)', subplot_labels)
plot_with_error_bars(axs[1, 0], entropy_specter_means, entropy_specter_means_ns, 'Mean Author Entropy (SPECTER)', subplot_labels)

# Plot Mean Citation Diversities per Bin with and without superstars
plot_with_error_bars(axs[0, 1], cit_diversity_lda_means, cit_diversity_lda_means_ns, 'Mean Author Citation Diversity (LDA)', subplot_labels)
plot_with_error_bars(axs[1, 1], cit_diversity_specter_means, cit_diversity_specter_means_ns, 'Mean Author Citation Diversity (SPECTER)', subplot_labels)

# Plot Mean Entropies per Bin with and without superstars
plot_with_error_bars(axs[0, 2], ref_diversity_lda_means, ref_diversity_lda_means_ns, 'Mean Author Reference Diversity (LDA)', subplot_labels)
plot_with_error_bars(axs[1, 2], ref_diversity_specter_means, ref_diversity_specter_means_ns, 'Mean Author Reference Diversity (SPECTER)', subplot_labels)

plot_with_error_bars(axs[0, 3], pair_diversity_lda_means, pair_diversity_lda_means_ns, 'Mean Author Pairwise Diversity (LDA)', subplot_labels)
plot_with_error_bars(axs[1, 3], pair_diversity_specter_means, pair_diversity_specter_means_ns, 'Mean Author Pairwise Diversity (SPECTER)', subplot_labels)

fig.supxlabel("Fraction of Author's Publications that Cites a Superstar", fontsize=16)



# Adjust layout
plt.tight_layout()

plt.savefig('plots/embedding_metrics_by_influence_group.png')
# Show plot
plt.show()