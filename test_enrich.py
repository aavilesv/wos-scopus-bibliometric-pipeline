import pandas as pd
import numpy as np
import config
from file_validation import build_default_paths
from loaders import load_merge_scopus, load_merge_wos
from sjr_analysis import _clean_categories, _best_title_match
import sys

def test_enrich():
    paths = build_default_paths()
    scimago_df = pd.read_csv(paths.scimago_file, sep=";")
    
    # We create a dummy combined_df to test the logic
    data = {
        "Title": ["Test 1", "Test 2", "Test 3", "Test 4", "Test 5"],
        "Source title": ["Nature", "Science", "Fake Journal", "Nature", "Cell"],
        "ISSN": ["0028-0836", "0036-8075", "1234-5678", np.nan, "0092-8674"],
        "Year": [2005, 2015, 2020, 2025, 2013] # Note years out of bounds!
    }
    combined_df = pd.DataFrame(data)
    
    before_len = len(combined_df)
    
    if scimago_df is None or scimago_df.empty:
        return combined_df

    scimago = scimago_df.copy()
    scimago = scimago.rename(columns={"Title": "Source title"})
    scimago["Source title"] = (
        scimago["Source title"]
        .astype(str)
        .str.replace(r"\([^)]*\)", "", regex=True)
        .str.strip()
    )

    if "Categories" in scimago.columns:
        scimago["Categories"] = scimago["Categories"].apply(_clean_categories)

    scimago_exp = scimago.assign(
        Issn=scimago["Issn"].astype(str).str.split(",")
    ).explode("Issn")
    scimago_exp["Issn"] = scimago_exp["Issn"].astype(str).str.strip()

    # Get min and max year of SCImago
    min_year = scimago_exp["Year"].min()
    max_year = scimago_exp["Year"].max()

    # Create Match_Year
    # We clip the Year column to the available SCImago years
    temp_df = combined_df.copy()
    temp_df["Match_Year"] = temp_df["Year"].astype(float).clip(lower=min_year, upper=max_year).fillna(max_year).astype(int)
    
    # Drop duplicates in scimago_exp to avoid explosion
    scimago_issn = scimago_exp.drop_duplicates(subset=["Issn", "Year"]).copy()
    scimago_title = scimago_exp.drop_duplicates(subset=["Source title", "Year"]).copy()
    
    # Merge 1: ISSN + Match_Year
    merged = pd.merge(
        temp_df,
        scimago_issn,
        how="left",
        left_on=["ISSN", "Match_Year"],
        right_on=["Issn", "Year"],
        suffixes=("", "_scimago")
    )
    
    # Merge 2: Match fuzzy Title + Match_Year for unmatched
    unmatched_mask = merged["Issn"].isna()
    
    if unmatched_mask.any():
        scimago_titles = scimago_title["Source title"].dropna().astype(str).unique().tolist()
        
        # We need to apply fuzzy match only for unmatched
        titles_to_match = merged.loc[unmatched_mask, "Source title"]
        matched_titles = titles_to_match.apply(lambda x: _best_title_match(x, scimago_titles, 90))
        
        merged.loc[unmatched_mask, "Fuzzy_Title"] = matched_titles
        
        # Now resolve those with left merge
        fuzzy_merged = pd.merge(
            merged[unmatched_mask],
            scimago_title,
            how="left",
            left_on=["Fuzzy_Title", "Match_Year"],
            right_on=["Source title", "Year"],
            suffixes=("", "_scimago2")
        )
        
        # Combine the columns
        for col in scimago_issn.columns:
            if col in ["Issn", "Year", "Source title"]:
                continue
            base_col = f"{col}_scimago"
            new_col = f"{col}_scimago2"
            if base_col in merged.columns and new_col in fuzzy_merged.columns:
                # Update merged with fuzzy matched
                merged.loc[unmatched_mask, base_col] = fuzzy_merged[new_col].values
                
    # Final cleanup
    SCIMAGO_MERGE_MAP = {
        "SJR": "SJR_scimago",
        "SJR Best Quartile": "SJR Best Quartile_scimago",
        "H index": "H index_scimago",
        "Country": "Country_scimago",
        "Region": "Region_scimago",
        "Publisher": "Publisher_scimago",
        "Categories": "Categories_scimago",
        "Areas": "Areas_scimago",
    }
    
    for final_col, sc_col in SCIMAGO_MERGE_MAP.items():
        if sc_col in merged.columns:
            if final_col not in merged.columns:
                merged[final_col] = None
            merged[final_col] = merged[final_col].combine_first(merged[sc_col])
    
    cols_to_drop = [c for c in merged.columns if c.endswith("_scimago") or c in ["Match_Year", "Fuzzy_Title", "Issn"]]
    
    # Also drop 'Year_scimago' and 'Source title_scimago' if exists
    cols_to_drop.extend(["Year_scimago", "Source title_scimago"])
    enriched = merged.drop(columns=[c for c in cols_to_drop if c in merged.columns])
    
    print("Length before:", before_len)
    print("Length after:", len(enriched))
    print(enriched[["Title", "Year", "SJR", "H index"]])
    
test_enrich()
