import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow as pa
import pandas as pd
import matplotlib.pyplot as plt

def load_and_join_datasets(detail_dir, row_size_path):
    # Load idp_volume_detail dataset
    dataset = ds.dataset(detail_dir, format='csv', partitioning="hive")
    idp_volume_detail_table = dataset.to_table()

    # Load row_size dataset
    row_size_table = csv.read_csv(row_size_path)

    # Join datasets
    joined_table = pa.compute.join(idp_volume_detail_table, row_size_table, 'table_name', 'table_name', how='left')
    joined_table = joined_table.append_column('average_row_size', joined_table['avg_size'])
    joined_table = joined_table.append_column('total_size', pc.multiply(joined_table['row_count'], joined_table['average_row_size']))
    return joined_table

def summarize_data(joined_table):
    # Summarize into total_size_by_app
    total_size_by_app = joined_table.group_by(['calc_dt', 'src_appl_nm']).aggregate([
        ('row_count', 'sum'),
        ('total_size', 'sum')
    ])

    # Create app_monthly dataset
    calc_mm = pc.utf8_slice_codeunits(joined_table['calc_dt'], 0, 6)
    joined_table = joined_table.append_column('calc_mm', calc_mm)
    app_monthly = joined_table.group_by(['calc_mm', 'src_appl_nm']).aggregate([
        ('total_size', 'sum')
    ])
    return total_size_by_app, app_monthly

def save_datasets(total_size_by_app, app_monthly):
    # Save datasets to CSV
    csv.write_csv(total_size_by_app, 'total_size_by_app.csv')
    csv.write_csv(app_monthly, 'monthly.csv')

def plot_stacked_bar_chart():
    # Load app_monthly data
    df = pd.read_csv('monthly.csv')

    # Pivot for plotting
    pivot_df = df.pivot(index='calc_mm', columns='src_appl_nm', values='total_size')

    # Plot
    pivot_df.plot(kind='bar', stacked=True, figsize=(10, 6))
    plt.title('Sum(Total Size) by Application per Month')
    plt.xlabel('Month (Calc_MM)')
    plt.ylabel('Sum(Total Size)')
    plt.xticks(rotation=45)
    plt.legend(title='Application', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

def main():
    detail_dir = 'path_to_idp_volume_detail_directory'
    row_size_path = 'path_to_row_size.csv'

    # Load and join datasets
    joined_table = load_and_join_datasets(detail_dir, row_size_path)

    # Summarize data
    total_size_by_app, app_monthly = summarize_data(joined_table)

    # Save summarized datasets
    save_datasets(total_size_by_app, app_monthly)

    # Plot stacked bar chart
    plot_stacked_bar_chart()

if __name__ == '__main__':
    main()
