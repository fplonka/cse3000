import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pd.options.display.max_columns = None

# Load datasets
papers = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/papers/all_cs_papers_simple.ndjson', lines=True)
authors = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/authors_parallel/all_authors_simple2.ndjson', lines=True)
innovation = pd.read_csv('/Users/filip/Code/rp/rdataprep/w6/innovation_scores.csv')

# drop null publicationdate (~20%, oops)
papers = papers.dropna(subset=['publicationdate'])
papers['publicationdate'] = pd.to_datetime(papers['publicationdate'])
papers_og = papers.copy()

num_years = 40 + 1

papers = papers[['corpusid', 'authorIds', 'publicationdate', 'citationcount']]
authors = authors[['authorid', 'hindex']]
innovation = innovation[['corpusid', 'innovation_score']]

def print_df_info(df, name):
    print(f"DataFrame: {name}")
    print(f"Types: \n{df.dtypes}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Samples: \n{df.head()}")
    print("------------------------------------------------")

print("datasets loaded:")
print_df_info(papers, 'papers_df')
print_df_info(authors, 'authors_df')
print_df_info(innovation, 'innovation_df')

# Step 2: Merge the datasets
# Merge papers and innovation datasets on corpusid
papers = papers.merge(innovation, on='corpusid', how='inner')

# Step 3: Identify Superstars
# Define a threshold for superstars
hindex_threshold = authors['hindex'].quantile(0.999)
superstars = authors[authors['hindex'] >= hindex_threshold]['authorid'].tolist()

# Step 4: Identify Early Collaborators
# Extract the first publication year for each author
author_first_pub = papers.explode('authorIds').groupby('authorIds')['publicationdate'].min().reset_index()
author_first_pub.columns = ['authorid', 'first_pub_date']

print_df_info(author_first_pub, "author first pub")

author_first_pub.to_csv('author_first_pub.csv')

# Merge the first publication year back to the papers dataset
papers = papers.explode('authorIds')
papers = papers.merge(author_first_pub, left_on='authorIds', right_on='authorid')


author_papers_first_5_years = papers[papers['publicationdate'] <= (papers['first_pub_date'] + np.timedelta64(52*5, 'W'))]
author_papers_first_5_years.to_csv('author_papers_first_5_years.csv')

paper_authors = papers_og[['corpusid', 'authorIds']]

author_collabs = author_papers_first_5_years.merge(paper_authors, on='corpusid').groupby('authorid')['authorIds_y'].apply(list)
print_df_info(author_collabs, "author collabs")
author_collabs.to_csv('author_collabs.csv')

superstars = set(superstars)

def is_early_collaborator(papers_authors_list, superstars):
    total_papers = len(papers_authors_list)
    collab_with_superstar = 0
    
    for paper_authors in papers_authors_list:
        if any(author in superstars for author in paper_authors):
            collab_with_superstar += 1
    
    return collab_with_superstar >= (total_papers / 2)

author_collabs = author_collabs.reset_index()
author_collabs['is_collaborator'] = author_collabs['authorIds_y'].apply(lambda x: is_early_collaborator(x, superstars))

early_collaborators = author_collabs[author_collabs['is_collaborator']]['authorid'].tolist()
early_collaborators = set(early_collaborators)
early_collaborators = early_collaborators - superstars

print(f"Number of superstars: {len(superstars)}")
print(f"Number of early collaborators: {len(early_collaborators)}")

# Step 5: Identify Early Innovators

# Cheating -- computed this in Elixir Livebook since it was easier; might redo in Python later
early_innovators = pd.read_csv('early_innovator_ids.csv')['authorid'].tolist()
print(len(early_innovators))
early_innovators = set(early_innovators) - superstars

# print(f"Number of early innovators: {len(set(early_innovators['authorid'].tolist()) - superstars)}")
print(f"Number of early innovators: {len(early_innovators)}")


# Step 6: Find changes over time. Huge pain. Pseudocode is as follows:
# join citations with their time based on the publicationdate of citingcorpusid.
# for each author A:
#   get t0.
#   let P_A = papers by that author
#   let C_A = citations where the cited paper is a paper in P_A
#   for each t in 0..25:
#     let C_A' = C_A filtered to citations within t of t0
#     join P_A with C_A', group by paper id, count citing corpus ids, then finally find mean for that paper
#     store that value as result[A][t]
# 
# for t in 0..25:
#   find avg result[A][t] over all A
#   plot that value
#   
# do all of the above once for authors A who are early innovators, and another time for early collaborators.

# Load citation data
citations = pd.read_json('/Users/filip/Code/rp/rdataprep/datasets/citations/all_citations_simple2.ndjson', lines=True)

# Merge citations with publication dates of citing papers
citations = citations.merge(papers[['corpusid', 'publicationdate']], left_on='citingcorpusid', right_on='corpusid', suffixes=('', '_citing'))
# citations = citations.rename(columns={'publicationdate_citing': 'citing_publicationdate'}).drop(columns=['corpusid'])

print_df_info(citations, "CITATIONS")

# Function to calculate mean citations per paper for each author over time

def calculate_citations_per_paper(author_ids, papers_exploded, citations, superstars, exclude_superstars=False):
    results = {author: [] for author in author_ids}
    cnt = 0
    
    if exclude_superstars:
        print("filtering.....")
        superstar_corpusids = papers_exploded[papers_exploded['authorIds'].isin(superstars)]['corpusid'].unique()
        print("superstar paper ids:", superstar_corpusids[0:100])
        papers_exploded = papers_exploded[~papers_exploded['corpusid'].isin(superstar_corpusids)]
        # papers_exploded = papers_exploded[~papers_exploded['authorIds'].isin(superstars)]

        print("done")
    
    print_df_info(papers_exploded, "papers exploded")

    for cnt, author in enumerate(author_ids, start=1):
        print(f"at {cnt} out of {len(author_ids)}")
        
        author_papers = papers_exploded.loc[papers_exploded['authorIds'] == author]
        t0 = author_papers['publicationdate'].min()
        author_corpusids = author_papers['corpusid'].unique()
        
        author_citations = citations[
                (citations['citedcorpusid'].isin(author_corpusids))
        ]
        
        total_citations = 0
        for t in range(num_years):
            time_limit = t0 + np.timedelta64(t * 52, 'W')  # t in weeks
            
            total_citations = author_citations[
                (author_citations['publicationdate'] <= time_limit)
            ].count()

            results[author].append(total_citations)

    return results

# Calculate for early collaborators and early innovators

# Take a random subset of 100 author IDs
sample_size = 10000
sampled_early_collaborators = set(np.random.choice(list(early_collaborators), min(sample_size, len(early_collaborators)), replace=False))
sampled_early_innovators = set(np.random.choice(list(early_innovators), min(sample_size, len(early_innovators)), replace=False))

papers_exploded = papers_og.explode('authorIds')

print(sampled_early_collaborators)
print(sampled_early_innovators)

early_collaborator_citations = calculate_citations_per_paper(sampled_early_collaborators, papers_exploded, citations, superstars, False)
early_innovator_citations = calculate_citations_per_paper(sampled_early_innovators, papers_exploded, citations, superstars, False)

import pickle

data_to_save = {
    'early_collaborator_citations': early_collaborator_citations,
    'early_innovator_citations': early_innovator_citations
}

# Save the data to a file
with open('data_with_superstars_top01percentsuperstars.pkl', 'wb') as f:
    pickle.dump(data_to_save, f)

print("Data saved to data_with_superstars_top01percentsuperstars.pkl")

# Average the results over all authors for each group
def average_citations_over_time(citation_results):
    avg_citations = []
    for t in range(num_years):
        avg_citations.append(np.mean([citations[t] for citations in citation_results.values()]))
    return avg_citations

avg_citations_collaborators = average_citations_over_time(early_collaborator_citations)
avg_citations_innovators = average_citations_over_time(early_innovator_citations)

# Step 7: Plot the results
plt.figure(figsize=(10, 6))
plt.plot(range(num_years), avg_citations_collaborators, label='Early Collaborators')
plt.plot(range(num_years), avg_citations_innovators, label='Early Innovators')
plt.xlabel('Years from First Publication (t - t0)')
plt.ylabel('Average Citations Per Paper')
plt.title('Citations Per Paper Over Time for Early Collaborators and Innovators')
plt.legend()
plt.grid(True)
plt.savefig('citations_per_paper_over_time.png')
plt.show()