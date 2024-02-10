import pyarrow.csv as pcsv
import pyarrow.compute as pcompute
import pyarrow as pa
import os

# Step 1: Read and Concatenate .csv Files
def read_and_concatenate_csv(directory):
    tables = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                table = pcsv.read_csv(file_path)
                # Check if the Table is not empty and has the required columns
                if table.num_rows > 0 and set(['calc_dt', 'table_name', 'src_appl_nm', 'row_count']).issubset(table.column_names):
                    tables.append(table)
            except pa.lib.ArrowInvalid:
                continue  # Skip empty or invalid files
    return pa.concat_tables(tables) if tables else None

idp_volume_detail_table = read_and_concatenate_csv('path_to_idp_volume_detail_directory')

# Step 2: Read the `row_size` Dataset
row_size_table = pcsv.read_csv('path_to_row_size.csv')

# Step 4: Join Tables
joined_table = pa.compute.join(idp_volume_detail_table, row_size_table, 'table_name', 'table_name', how='left')

# Step 5: Calculate Additional Columns
average_row_size = joined_table['avg_size']
total_size = pcompute.multiply(joined_table['row_count'], average_row_size)
joined_table = joined_table.append_column('average_row_size', average_row_size)
joined_table = joined_table.append_column('total_size', total_size)

# Step 6: Save Result
pa.csv.write_csv(joined_table, 'size_detail.csv')

# Step 7: Print Stats
print(f"Number of rows: {joined_table.num_rows}")
total_size_column = joined_table.column('total_size')
print(f"Average total size: {pcompute.mean(total_size_column).as_py()}")
print(f"Minimum total size: {pcompute.min_max(total_size_column).as_py()['min']}")
print(f"Maximum total size: {pcompute.min_max(total_size_column).as_py()['max']}")
