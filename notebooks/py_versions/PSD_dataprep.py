# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: os_data_pipeline
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Power Station Dictionary (PSD) Data Prep <br>
# This notebook extracts the latest data from the PSD, splits out the differnet sett_bmu_ids and joins the power stations names and locations. <br>
# It should be run first, before the pipeline. However, it can be run less frequently than the pipeline.

# %%
import pandas as pd
import requests
import os
import numpy as np
import pipeline_fns as plfns

osdp_folder = os.environ.get("OSDP")
osdp_folder

# %%
# Specify which folder contains your local directory
(
    location,
    location_BMRS,
    location_BMRS_PHYBMDATA,
    location_BMRS_B1610,
    location_BMRS_Final,
) = plfns.create_folder_structure(osdp_folder=osdp_folder)

# %%
# Read in the different datasets from the PSD repo
url_ids = "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/shiro/data/dictionary/ids.csv"
url_locations = "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/shiro/data/attribute_sources/plant-locations/plant-locations.csv"
url_common_names = "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/shiro/data/attribute_sources/common-names/common-names.csv"
fuel_types_psd = "https://raw.githubusercontent.com/OSUKED/Power-Station-Dictionary/shiro/data/attribute_sources/bmu-fuel-types/fuel_types.csv"
fuel_types_elexon = "https://www.bmreports.com/bmrs/cloud_doc/BMUFuelType.xls"

df_ids = pd.read_csv(url_ids, usecols=["dictionary_id", "sett_bmu_id", "ngc_bmu_id"])
df_ids = df_ids[df_ids["ngc_bmu_id"].notna()]  # Drop any older power stations that don't have a Settlement BMU ID
df_locations = pd.read_csv(url_locations)
df_common_names = pd.read_csv(url_common_names)
df_fuel_types_psd = pd.read_csv(fuel_types_psd)

# For the fuel types which are an Excel file on the web
resp = requests.get(fuel_types_elexon)
with open(os.path.join(location, "BMUFuelType.xls"), "wb") as output:
    output.write(resp.content)
df_fuel_types_elexon = pd.read_excel("../data/BMUFuelType.xls")

# %%
# Split the IDs dataset so that each of the Settlement BMU IDs becomes its own row
df_ngc_ids_long = (
    pd.DataFrame(df_ids["ngc_bmu_id"].str.split(",").tolist(), index=df_ids["dictionary_id"])
    .stack()
    .reset_index()
    .drop(columns="level_1")
    .rename(columns={0: "ngc_bmuID"})
)
df_ngc_ids_long["ngc_bmuID"] = df_ngc_ids_long["ngc_bmuID"].str.strip()

df_sett_ids_long = (
    pd.DataFrame(df_ids["sett_bmu_id"].str.split(",").tolist(), index=df_ids["dictionary_id"])
    .stack()
    .reset_index()
    .drop(columns="level_1")
    .rename(columns={0: "sett_bmuID"})
)
df_sett_ids_long["sett_bmuID"] = df_sett_ids_long["sett_bmuID"].str.strip()
df_sett_ids_long["sett_ngc_bmu_matching_ID"] = df_sett_ids_long["sett_bmuID"].str.slice(start=2)

# %%
# Merge the exploded IDs dataset with the common names, locations and psd fuel types
df_psd_merged = df_sett_ids_long.merge(df_common_names, how="left", on="dictionary_id").merge(
    df_locations, how="left", on="dictionary_id"
)
df_psd_merged = df_psd_merged.merge(
    df_fuel_types_psd, how="left", left_on="sett_ngc_bmu_matching_ID", right_on="ngc_bmu_id"
)

# Merge the fuel types based on the BMU ID and the SETT_BMU_ID
df_psd_merged = df_psd_merged.merge(
    df_fuel_types_elexon[["SETT_BMU_ID", "FUEL TYPE"]], how="left", left_on="sett_bmuID", right_on="SETT_BMU_ID"
)

# Set the final fuel type from the two datasets
df_psd_merged["fuel"] = np.where(
    df_psd_merged["FUEL TYPE"].isnull(), df_psd_merged["fuel_type"], df_psd_merged["FUEL TYPE"]
)
df_psd_merged = df_psd_merged.drop(
    columns=["sett_ngc_bmu_matching_ID", "fuel_type", "comments", "SETT_BMU_ID", "FUEL TYPE"]
)

# %%
# Write the merged dataset to the repo
df_psd_merged.to_csv(os.path.join(location, "merged_psd.csv"))
