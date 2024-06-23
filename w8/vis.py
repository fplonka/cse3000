import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Reading the NDJSON files with pandas
file1 = 'ns_diversity_specter.ndjson'
file2 = 'ss_diversity_specter.ndjson'

metric_type = "Diversity"

df1 = pd.read_json(file1, lines=True)
df2 = pd.read_json(file2, lines=True)

# Extracting the metrics
metrics1 = df1['diversity']
metrics2 = df2['diversity']

# Calculating means
mean1 = metrics1.mean()
mean2 = metrics2.mean()

print("mean ratio:", mean2 / mean1)

# Defining common bin edges
min_metric = min(metrics1.min(), metrics2.min())
max_metric = max(metrics1.max(), metrics2.max())
bins = np.linspace(min_metric, max_metric, 40)

# Plotting the distributions
plt.figure(figsize=(10, 6))

plt.hist(metrics1, bins=bins, alpha=0.5, label=f'Non-Superstar {metric_type} Diversity (Mean={mean1:.3f})', color='blue', density=True)
plt.hist(metrics2, bins=bins, alpha=0.5, label=f'Superstar {metric_type} Diversity (Mean={mean2:.3f})', color='green', density=True)

# Adding means to the plot
plt.axvline(mean1, color='blue', linestyle='dashed', linewidth=1)
plt.axvline(mean2, color='green', linestyle='dashed', linewidth=1)

# Adding labels and title
plt.xlabel(f'Proposed Diversity Metric Value')
plt.ylabel('Density')
plt.title(f'Distribution of Mean {metric_type}: Superstars vs. Non-Superstars (Using SPECTER Embeddings)')
plt.legend()

# Showing the plot
plt.show()