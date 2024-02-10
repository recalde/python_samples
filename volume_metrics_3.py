import pyarrow.csv as pcsv
import pyarrow.compute as pcompute
import pyarrow as pa

# Assuming 'joined_table' is already defined and populated from previous steps

# Step 1: Summarize `table_size_by_app` into `total_size_by_app`
group_keys = ['calc_dt', 'src_appl_nm']
total_size_by_app = joined_table.group_by(group_keys).aggregate([
    ('row_count', 'sum'),
    ('total_size', 'sum')
])

# Step 2: Save `total_size_by_app` Dataset
pa.csv.write_csv(total_size_by_app, 'total_size_by_app.csv')

# Step 3: Create `app_monthly` Dataset
# Extract the first 6 characters of `calc_dt` for month and year (calc_mm)
calc_mm = pcompute.utf8_slice_codeunits(joined_table['calc_dt'], 0, 6)
joined_table = joined_table.append_column('calc_mm', calc_mm)

group_keys_monthly = ['calc_mm', 'src_appl_nm']
app_monthly = joined_table.group_by(group_keys_monthly).aggregate([
    ('total_size', 'sum')
])

# Step 4: Save `app_monthly` Dataset
pa.csv.write_csv(app_monthly, 'monthly.csv')

# Print a confirmation message
print("Datasets 'total_size_by_app.csv' and 'monthly.csv' have been created.")
