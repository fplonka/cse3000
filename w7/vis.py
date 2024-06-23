import pickle
import numpy as np
import matplotlib.pyplot as plt

def average_citations_over_time(citation_results):
    avg_citations = []
    for t in range(26):
        avg_citations.append(np.mean([citations[t] for citations in citation_results.values()]))
    return avg_citations

# Load the data for no superstars
# with open('data_no_superstars.pkl', 'rb') as f:
with open('data_no_superstars_top1percentsuperstars.pkl', 'rb') as f:
    data_no_superstars = pickle.load(f)

early_collaborator_citations_no_superstars = data_no_superstars['early_collaborator_citations']
early_innovator_citations_no_superstars = data_no_superstars['early_innovator_citations']

avg_citations_collaborators_no_superstars = average_citations_over_time(early_collaborator_citations_no_superstars)
avg_citations_innovators_no_superstars = average_citations_over_time(early_innovator_citations_no_superstars)

# Load the data for with superstars
# with open('data_with_superstars.pkl', 'rb') as f:
# with open('data_with_superstars_top1percentsuperstars.pkl', 'rb') as f:
with open('data_with_superstars_top1percentsuperstars.pkl', 'rb') as f:
    data_with_superstars = pickle.load(f)

early_collaborator_citations_with_superstars = data_with_superstars['early_collaborator_citations']
early_innovator_citations_with_superstars = data_with_superstars['early_innovator_citations']

avg_citations_collaborators_with_superstars = average_citations_over_time(early_collaborator_citations_with_superstars)
avg_citations_innovators_with_superstars = average_citations_over_time(early_innovator_citations_with_superstars)

# Create the plots side by side
plt.figure(figsize=(14, 6))

# Plot for data with superstars
plt.subplot(1, 2, 1)
plt.plot(range(26), avg_citations_collaborators_with_superstars, label='Early Collaborators')
plt.plot(range(26), avg_citations_innovators_with_superstars, label='Early Innovators')
plt.xlabel('Years from First Publication (t - t0)')
plt.ylabel('Average Citations Per Paper')
plt.title('Including Superstar Papers')
plt.legend()
plt.grid(True)

# Plot for data without superstars
plt.subplot(1, 2, 2)
plt.plot(range(26), avg_citations_collaborators_no_superstars, label='Early Collaborators')
plt.plot(range(26), avg_citations_innovators_no_superstars, label='Early Innovators')
plt.xlabel('Years from First Publication (t - t0)')
plt.ylabel('Average Citations Per Paper')
plt.title('Excluding Superstar Papers')
plt.legend()
plt.grid(True)

# Save and show the figure
plt.suptitle('Citations Per Paper Over Time for Early Collaborators and Innovators')
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('citations_per_paper_over_time_comparison.png')
plt.show()