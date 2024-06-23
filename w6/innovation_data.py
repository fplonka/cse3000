import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv('innovation_scores.csv')

# Convert the publication_date column to datetime
df['publication_date'] = pd.to_datetime(df['publication_date'])

# Extract the year from the publication_date
df['year'] = df['publication_date'].dt.year

print(df['year'].iloc[:100])

# Calculate the average innovation score per year
average_innovation_per_year = df.groupby('year')['innovation_score'].mean()

print(average_innovation_per_year)

# Create a figure with two subplots side by side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Plot the average innovation score per year on the first subplot
ax1.plot(average_innovation_per_year.index, average_innovation_per_year.values, marker='o')
ax1.set_xlabel('Year')
ax1.set_ylabel('Average Innovation Score')
ax1.set_title('Average Innovation Score per Year')
ax1.grid(True)

# Limit the data to the desired x-axis range for the histogram
limited_innovation_scores = df[df['innovation_score'] <= 500]['innovation_score']

# Plot a histogram of the limited innovation scores on the second subplot
ax2.hist(limited_innovation_scores, bins=100, edgecolor='black')
ax2.set_xlabel('Innovation Score')
ax2.set_ylabel('Frequency')
ax2.set_title('Histogram of Innovation Scores')

# Adjust layout to prevent overlap
plt.tight_layout()

# Show the plots
plt.show()
