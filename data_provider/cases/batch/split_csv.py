import polars as pl
import random
from pathlib import Path

def split_csv(filename: str, n_files: int, output_dir: str):
    df = pl.read_csv(filename)

    total_rows = df.height
    
    points_cut = sorted(random.sample(range(1, total_rows), n_files - 1))
    points_cut = [0] + points_cut + [total_rows]
    
    for i in range(n_files):
        start = points_cut[i]
        end = points_cut[i + 1]
        
        df_chunk = df[start:end]
        name_output = f'{output_dir}/file_{i}_cases.csv'
        df_chunk.write_csv(name_output)
        print(f'File {name_output} created: {df_chunk.height} rows')


if __name__ == '__main__':
    script_dir = Path(__file__).parent
    split_csv(script_dir / '../../../datasets/cases/streaming/covid_cases.csv', 13, script_dir / '../../../datasets/cases/batches')
