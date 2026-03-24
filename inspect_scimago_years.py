import pandas as pd
import config
from file_validation import build_default_paths

paths = build_default_paths()
df = pd.read_csv(paths.scimago_file, sep=";")
print("Min year:", df['Year'].min())
print("Max year:", df['Year'].max())
print("Null years:", df['Year'].isna().sum())
print("Available years:", sorted(df['Year'].dropna().unique()))
