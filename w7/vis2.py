import pickle
import numpy as np
import matplotlib.pyplot as plt

num_years = 40 + 1

def average_citations_over_time(citation_results):
    avg_citations = []
    for t in range(num_years):
        avg_citations.append(np.mean([citations[t] for citations in citation_results.values()]))
    return avg_citations

def standard_error_over_time(citation_results):
    std_errors = []
    for t in range(num_years):
        std_errors.append(np.std([citations[t] for citations in citation_results.values()]) / np.sqrt(len(citation_results)))
    return std_errors

# load the data for no superstars
with open('data_no_superstars_top01percentsuperstars.pkl', 'rb') as f:
    data_no_superstars = pickle.load(f)

early_collaborator_citations_no_superstars = data_no_superstars['early_collaborator_citations']
early_innovator_citations_no_superstars = data_no_superstars['early_innovator_citations']

avg_citations_collaborators_no_superstars = average_citations_over_time(early_collaborator_citations_no_superstars)
avg_citations_innovators_no_superstars = average_citations_over_time(early_innovator_citations_no_superstars)

stderr_citations_collaborators_no_superstars = standard_error_over_time(early_collaborator_citations_no_superstars)
stderr_citations_innovators_no_superstars = standard_error_over_time(early_innovator_citations_no_superstars)

# load the data for with superstars
with open('data_with_superstars_top01percentsuperstars.pkl', 'rb') as f:
    data_with_superstars = pickle.load(f)

early_collaborator_citations_with_superstars = data_with_superstars['early_collaborator_citations']
early_innovator_citations_with_superstars = data_with_superstars['early_innovator_citations']

avg_citations_collaborators_with_superstars = average_citations_over_time(early_collaborator_citations_with_superstars)
avg_citations_innovators_with_superstars = average_citations_over_time(early_innovator_citations_with_superstars)

stderr_citations_collaborators_with_superstars = standard_error_over_time(early_collaborator_citations_with_superstars)
stderr_citations_innovators_with_superstars = standard_error_over_time(early_innovator_citations_with_superstars)

print("no superstars:")
print("    collab:", avg_citations_collaborators_no_superstars)
print("    innov: ", avg_citations_innovators_no_superstars)
print("with superstars:")
print("    collab:", avg_citations_collaborators_with_superstars)
print("    innov: ", avg_citations_innovators_with_superstars)

# create the plots side by side
fig = plt.figure(figsize=(14, 6))

# plot for data with superstars
plt.subplot(1, 2, 1)
plt.plot(range(num_years), avg_citations_collaborators_with_superstars, label='early collaborators')
plt.fill_between(range(num_years), 
                 np.array(avg_citations_collaborators_with_superstars) - np.array(stderr_citations_collaborators_with_superstars),
                 np.array(avg_citations_collaborators_with_superstars) + np.array(stderr_citations_collaborators_with_superstars), 
                 color='blue', alpha=0.2)
plt.plot(range(num_years), avg_citations_innovators_with_superstars, label='early innovators')
plt.fill_between(range(num_years), 
                 np.array(avg_citations_innovators_with_superstars) - np.array(stderr_citations_innovators_with_superstars),
                 np.array(avg_citations_innovators_with_superstars) + np.array(stderr_citations_innovators_with_superstars), 
                 color='orange', alpha=0.2)
# plt.xlabel('Years from First Publication (t - t0)')
plt.ylabel('Average Citations Per Publication')
plt.title('Including Superstar Papers')
plt.legend()
# plt.grid(True)

# plot for data without superstars
plt.subplot(1, 2, 2)
plt.plot(range(num_years), avg_citations_collaborators_no_superstars, label='early collaborators')
plt.fill_between(range(num_years), 
                 np.array(avg_citations_collaborators_no_superstars) - np.array(stderr_citations_collaborators_no_superstars),
                 np.array(avg_citations_collaborators_no_superstars) + np.array(stderr_citations_collaborators_no_superstars), 
                 color='blue', alpha=0.2)
plt.plot(range(num_years), avg_citations_innovators_no_superstars, label='early innovators')
plt.fill_between(range(num_years), 
                 np.array(avg_citations_innovators_no_superstars) - np.array(stderr_citations_innovators_no_superstars),
                 np.array(avg_citations_innovators_no_superstars) + np.array(stderr_citations_innovators_no_superstars), 
                 color='orange', alpha=0.2)
# plt.xlabel('Years from First Publication (t - t0)')
plt.ylabel('Average Citations Per Publication')
plt.title('Excluding Superstar Papers')
plt.legend()
# plt.grid(True)

fig.supxlabel('Years from First Publication (t - t0)')

# save and show the figure
# plt.suptitle('citations per paper over time for early collaborators and innovators')
# plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.tight_layout()
plt.savefig('citations_per_paper_over_time_comparison.png')
plt.show()
