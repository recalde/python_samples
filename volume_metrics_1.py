import pandas as pd
import os

# Step 1: Read and Concatenate .csv Files
def read_and_concatenate_csv(directory):
    all_files = os.listdir(directory)
    df_list = []
    for filename in all_files:
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                # Ensure the DataFrame is not empty and has the required columns
                if not df.empty and set(['calc_dt', 'table_name', 'src_appl_nm', 'row_count']).issubset(df.columns):
                    df_list.append(df)
            except pd.errors.EmptyDataError:
                continue  # Skip empty files
    return pd.concat(df_list, ignore_index=True)

idp_volume_detail_df = read_and_concatenate_csv('path_to_idp_volume_detail_directory')

# Step 2: Read the `row_size` Dataset
row_size_df = pd.read_csv('path_to_row_size.csv')

# Step 3: Data Cleaning (if necessary, this step assumes the data is already clean)

# Step 4: Join DataFrames
result_df = pd.merge(idp_volume_detail_df, row_size_df, on='table_name', how='left')

# Step 5: Calculate Additional Columns
result_df['average_row_size'] = result_df['avg_size']  # Assuming 'avg_size' is the column from row_size_df
result_df['total_size'] = result_df['row_count'] * result_df['average_row_size']

# Step 6: Save Result
result_df.to_csv('size_detail.csv', index=False)

# Step 7: Print Stats
print(f"Number of rows: {len(result_df)}")
print(f"Average total size: {result_df['total_size'].mean()}")
print(f"Minimum total size: {result_df['total_size'].min()}")
print(f"Maximum total size: {result_df['total_size'].max()}")