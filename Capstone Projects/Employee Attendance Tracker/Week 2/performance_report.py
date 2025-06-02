import pandas as pd
import numpy as np

# Read the CSV file
df = pd.read_csv("attendance.csv")

# Convert clock-in and clock-out times to datetime
df['clockin'] = pd.to_datetime(df['clockin'])
df['clockout'] = pd.to_datetime(df['clockout'])

# Calculate total work hours
df['workhours'] = (df['clockout'] - df['clockin']).dt.total_seconds() / 3600

# Replace zero work hours to avoid division errors
df['workhours'].replace(0, np.nan, inplace=True)

# Calculate productivity score
df['productivity_score'] = df['taskscompleted'] / df['workhours']

# Group by employee and get average stats
summary = df.groupby('employeeid')[['workhours', 'productivity_score']].mean()

# Sort to find top and bottom performers
top_performers = summary.sort_values(by='productivity_score', ascending=False)
bottom_performers = summary.sort_values(by='productivity_score', ascending=True)

# Output summary
print("Top Performers:\n", top_performers.head())
print("\nBottom Performers:\n", bottom_performers.head())
