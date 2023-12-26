import pandas as pd
from sleep_misc import kripke

def create_df_from_csv(csv_filename):
    # Read the CSV file and use the line number as the index
    df = pd.read_csv(csv_filename, header=0,usecols=["activity","interval","wake","gt"])
    df = df[df["interval"] != "EXCLUDED"]
    # Rename the index to "line"
    # df.index.name = "line"
    
    return df

## I CHOOSE TO USE "GT" DATA AS LABEL INSTEAD OF "WAKE" DATA EVEN THOUGH I COULD NOT MERGE THE PSG INFO WITH THIS CSV.
# Example usage:
csv_filename = "/home/huseyin/bitirme/mesa-benchmark/data/mesa/processed/task2/mesa_1_task2.csv"  # Replace with the actual file path

result_df = create_df_from_csv(csv_filename)

# Print the resulting DataFrame
# print(result_df["gt"].tolist())
# print(result_df)
# print(result_df[result_df["wake"] != result_df["gt"]])

(a,b) = kripke(result_df)

result_df["kripke_pred"] = b

print(a)
print(b)

print(result_df)

print((result_df['kripke_pred'] != result_df['gt']).sum())