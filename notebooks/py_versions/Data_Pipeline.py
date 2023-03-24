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
# ## Live Generation Map: BRMS API Calls and Data Cleanup

# %%
# Import Libraries
from ElexonDataPortal import api
import pandas as pd
import os
import numpy as np
from datetime import timedelta
import pipeline_fns as plfns
import warnings

warnings.filterwarnings(action="ignore", category=UserWarning)

# %%
osdp_folder = os.environ.get("OSDP")
# osdp_folder

# %%
# Specify which folder contains your local directory
(
    location,
    location_BMRS,
    location_BMRS_PHYBMDATA,
    location_BMRS_B1610,
    location_BMRS_Final,
) = plfns.create_folder_structure(osdp_folder=osdp_folder)

# %% [markdown]
# ### Data Diff Querying / Change Data Capture (CDC)
# For both the B1610 data and the PHYBMDATA we want to check if these datasets already exist in the "OSDP" directory, and create them if not. <br> <br>
# We then want to check for the difference since the pipeline was last run as this will be more efficient than requesting all the historic and BM data each time the pipeline is run. <br><br>
# In order to achieve this, the script first checks for updates to the historic generation by BMU (B1610 report), i.e. whether new B1610 data is available since the pipeline was last run. NB, this report only updates once daily for one entire day. If the B1610 data hasn't been updated for longer than the "num_days" variable, then the function will automatically cause the  "get_setup_B1610_data" function to run to create a new dataset. <br><br>
# Once the B1610 data has been updated, the script then checks for updates to the Balancing Mechanism Physical data: it removes any data that has now been replaced with historic data, then proceeds to query for new physical data. The period queried will be the first period since the physical data was last queried until the end of the current day. NB, physical data is updated half-hourly. Hence, this script should eventually run every 30 min.<br><br>
# At the end, the updated dataframes overwrite the existing ones.

# %%
df_B1610 = plfns.setup_update_B1610_data(location_BMRS_B1610=location_BMRS_B1610, num_days=14, hist_days=45)

# %%
BM_start_date = pd.to_datetime(df_B1610["settlementDate"].max() + timedelta(days=1)).replace(tzinfo=None)

# %%
df_PHYBMDATA = plfns.setup_update_PHYBM_data(
    BM_start_date=BM_start_date, location_BMRS_PHYBMDATA=location_BMRS_PHYBMDATA
)

# %% [markdown]
# ### Reducing the BM data to follow a similar patterns as the historic data
# Next, the balancing mechanism data should be filtered and transformed so that it follows a similar pattern as the B1610 data. <br> <br>
# Abbreviations (https://www.bmreports.com/bmrs/?q=help/glossary): <br>
# * **FPN**: Final Physical Notification - "A Physical Notification is the best estimate of the level of generation or demand that a participant in the BM expects a BM Unit to export or import, respectively, in a Settlement Period."
# * **BOAL(F)**: Bid Offer Acceptance Level - subsequent "last minute" changes to this notified generation, e.g. due to curtailment or due to balancing demands. "A Bid-Offer Acceptance is a formalised representation of the purchase and/or sale of Offers and/or Bids (see Bid-Offer Data below) by the System Operator in its operation of the Balancing Mechanism."
# * **MEL**: Maximum Export Level - It is the maximum power export level of a particular BM Unit at a particular time. It is submitted as a series of point MW values and associated times.

# %%
df_fpn, df_mel, df_boal = plfns.filter_and_rename_physical_Data(df_PHYBMDATA)

# %% [markdown]
# The half-hourly or sub-half-hourly data is resampled to minutely resolution so that actions that happen at different times during each half-hour period can be joined together.

# %%
df_boal_long = plfns.convert_physical_data_to_long(df_boal)
unit_boal_resolved = plfns.resolve_applied_bid_offer_level(df_boal_long)

# %%
df_fpn_long = plfns.convert_physical_data_to_long(df_fpn)
unit_fpn_resolved = plfns.resolve_FPN_MEL_level(df_fpn_long)

# %%
df_mel_long = plfns.convert_physical_data_to_long(df_mel)
unit_mel_resolved = plfns.resolve_FPN_MEL_level(df_mel_long)

# %%
# After resampling the data to minutely resolution (Time), join the FPN, BOAL and MEL data.

df_fpn_boal = pd.merge(
    unit_fpn_resolved, unit_boal_resolved, how="outer", on=["Time", "bmUnitID"], suffixes=["_fpn", "_boal"]
)

df_fpn_mel_boal = pd.merge(df_fpn_boal, unit_mel_resolved, how="outer", on=["Time", "bmUnitID"]).rename(
    columns={"Level": "Level_mel"}
)

# %%
df_fpn_mel_boal["quantity"] = df_fpn_mel_boal["Level_boal"].fillna(
    df_fpn_mel_boal["Level_fpn"], inplace=False
)  # If a BOAL value exists, use it. Otherwise, retain the FPN value (which will always exist).
df_fpn_mel_boal["quantity"] = np.where(
    df_fpn_mel_boal["quantity"] > df_fpn_mel_boal["Level_mel"],
    df_fpn_mel_boal["Level_mel"],
    df_fpn_mel_boal["quantity"],
)  # If the MEL is lower than the BOAL or FPN value, cap the generation at the level of the MEL.

# %%
# Aggregate back up to the settlement period (SP) level and calculate the mean generation during each SP
df_fpn_mel_boal["settlementPeriod_fpn"] = df_fpn_mel_boal["settlementPeriod_fpn"].astype(str)
df_fpn_mel_boal_agg = (
    df_fpn_mel_boal.groupby(["local_datetime_fpn", "settlementDate", "settlementPeriod", "bmUnitID"])
    .mean()
    .reset_index()
)
df_fpn_mel_boal_agg = df_fpn_mel_boal_agg.rename(columns={"local_datetime_fpn": "local_datetime"})
df_fpn_mel_boal_agg = df_fpn_mel_boal_agg[
    ["local_datetime", "settlementDate", "settlementPeriod", "bmUnitID", "quantity"]
]

# %%
df_B1610["quantity"] = df_B1610["quantity"].astype("float")

# %%
df_generation = pd.concat((df_B1610, df_fpn_mel_boal_agg), axis=0)
df_generation = df_generation[
    df_generation["quantity"] > 0
].copy()  # Filter out BM data with a negative value (not a generator) or a value of 0 (B1610 only has positive values)

# %% [markdown]
# ### Merging the BMRS data with the Power Station Dictionary Names and Locations

# %%
df_psd_merged = pd.read_csv(os.path.join(location, "merged_psd.csv"), header=0, index_col=0)

# %%
df_generation = df_generation.merge(df_psd_merged, how="left", left_on="bmUnitID", right_on="sett_bmuID")
df_generation = df_generation[
    [
        "local_datetime",
        "settlementDate",
        "settlementPeriod",
        "bmUnitID",
        "quantity",
        "dictionary_id",
        "common_name",
        "longitude",
        "latitude",
        "fuel",
    ]
]
df_generation = df_generation.rename(
    columns={
        "local_datetime": "localDateTime",
        "bmUnitID": "BMUnitID",
        "dictionary_id": "dictionaryID",
        "common_name": "commonName",
    }
)

# %%
# Create default values for dashboard
df_generation["dictionaryID"] = np.where(df_generation["dictionaryID"].isnull(), 99999, df_generation["dictionaryID"])
df_generation["commonName"] = np.where(
    df_generation["commonName"].isnull(), "Unknown Name/Location", df_generation["commonName"]
)
df_generation["longitude"] = np.where(df_generation["longitude"].isnull(), -2.547855, df_generation["longitude"])
df_generation["latitude"] = np.where(df_generation["latitude"].isnull(), 54.00366, df_generation["latitude"])
df_generation["fuel"] = np.where(df_generation["fuel"].isnull(), "Unknown Fuel", df_generation["fuel"])


# Split data into renewable/non-renewable
df_generation["lowCarbonGeneration"] = np.where(
    df_generation["fuel"].isin(["BIOMASS", "NPSHYD", "NUCLEAR", "PS", "WIND", "Wind"]),
    "Low Carbon Generation",
    "Carbon Intensive Generation",
)
df_generation["renewableGeneration"] = np.where(
    df_generation["fuel"].isin(["BIOMASS", "NPSHYD", "PS", "WIND", "Wind"]),
    "Renewable Generation",
    "Non-Renewable Generation",
)

# Give the Fuel Types a more friendly name
fuel_type_friendly = {
    "BIOMASS": "Biomass",
    "CCGT": "Combined-cycle Gas Turbine",
    "COAL": "Coal",
    "OCGT": "Open-cycle Gas Turbine",
    "NPSHYD": "Other Hydro",
    "NUCLEAR": "Nuclear",
    "PS": "Pumped Storage Hydro",
    "WIND": "Wind",
    "Wind": "Wind",
}

df_generation["fuel"] = df_generation["fuel"].replace(to_replace=fuel_type_friendly)

# %%
df_generation.to_csv(os.path.join(location_BMRS_Final, "Generation_Combined.csv"), index=False)
