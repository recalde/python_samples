import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow as pa

# Step 1: Use PyArrow Dataset to read and concatenate .csv files
dataset = ds.dataset('path_to_idp_volume_detail_directory', format='csv', partitioning="hive")

# Filter out empty files and ensure required columns are present (optional, based on dataset specifics)
# dataset = dataset.filter(pc.field('row_count').is_not_null())

# Convert the dataset to a table
idp_volume_detail_table = dataset.to_table()

# Step 2: Read the `row_size` Dataset
row_size_table = csv.read_csv('path_to_row_size.csv')

# Join the tables on 'table_name' with a left join
joined_table = pa.compute.join(idp_volume_detail_table, row_size_table, 'table_name', 'table_name', how='left')

# Calculate 'average_row_size' and 'total_size'
joined_table = joined_table.append_column('average_row_size', joined_table['avg_size'])
joined_table = joined_table.append_column('total_size', pc.multiply(joined_table['row_count'], joined_table['average_row_size']))

# Follow the same steps for summarizing the data into 'total_size_by_app' and 'app_monthly'
# and for plotting the stacked bar chart as previously described

# Note: Adjust the file paths and ensure the directory structure and file names match your setup
