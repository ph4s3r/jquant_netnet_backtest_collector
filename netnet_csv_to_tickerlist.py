# ruff: noqa
import os
import csv
import re

# folder containing CSV files
folder_path = 'netnets'  # change to your folder path

# regex to extract date from filename like tse_netnets_2025-09-19.csv
filename_date_pattern = re.compile(r'_(\d{4}-\d{2}-\d{2})\.csv$')

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        match = filename_date_pattern.search(filename)
        if match:
            date_str = match.group(1).replace('-', '_')
            output_file = f'jquant_tickers_{date_str}.txt'
            input_path = os.path.join(folder_path, filename)

            with open(input_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                tickers = [row['ticker'] for row in reader]

            with open(output_file, 'w', encoding='utf-8') as f:
                for ticker in tickers:
                    f.write(f'{ticker}\n')

            print(f'Processed {filename} -> {output_file}')
