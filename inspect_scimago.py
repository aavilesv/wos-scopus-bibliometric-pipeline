import pandas as pd
import config
from file_validation import build_default_paths

paths = build_default_paths()
print(f"File: {paths.scimago_file}")
df = pd.read_csv(paths.scimago_file, sep=";", nrows=5)
print("Columns:", df.columns.tolist())
