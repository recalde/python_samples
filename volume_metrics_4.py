import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Read the Data
df = pd.read_csv('monthly.csv')

# Step 2: Pivot the Data
pivot_df = df.pivot(index='calc_mm', columns='src_appl_nm', values='total_size')

# Step 3: Plot
pivot_df.plot(kind='bar', stacked=True, figsize=(10, 6))
plt.title('Sum(Total Size) by Application per Month')
plt.xlabel('Month (Calc_MM)')
plt.ylabel('Sum(Total Size)')
plt.xticks(rotation=45)
plt.legend(title='Application', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Show the plot
plt.show()
