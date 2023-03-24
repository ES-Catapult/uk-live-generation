from ElexonDataPortal import api
from datetime import date, datetime, timedelta
import pytz
import pandas as pd
import pyarrow
import os


BMRS_API_KEY = os.environ["BMRS_API_KEY"]


client = api.Client(
    BMRS_API_KEY
)  # Copy API key in here (scripting key on the elexonportal.co.uk website; requires setting up user account)


def create_folder_structure(osdp_folder):
    """Creates the folder structure required to run the code

    Args:
        osdp_folder (str): The top level directory

    Returns:
        _type_: Returns a tuple of strings containing 5 paths used in the code
    """
    # Specify location of specific folders
    location = os.path.join(osdp_folder, "data")
    location_BMRS = os.path.join(location, "BMRS")
    location_BMRS_PHYBMDATA = os.path.join(location_BMRS, "PHYBMDATA")
    location_BMRS_B1610 = os.path.join(location_BMRS, "B1610")
    location_BMRS_Final = os.path.join(location_BMRS, "Final")

    # Make folders if they do not exist
    if not os.path.exists(location):
        os.mkdir(location)
    if not os.path.exists(location_BMRS):
        os.mkdir(location_BMRS)
    if not os.path.exists(location_BMRS_PHYBMDATA):
        os.mkdir(location_BMRS_PHYBMDATA)
    if not os.path.exists(location_BMRS_B1610):
        os.mkdir(location_BMRS_B1610)
    if not os.path.exists(location_BMRS_Final):
        os.mkdir(location_BMRS_Final)

    return location, location_BMRS, location_BMRS_PHYBMDATA, location_BMRS_B1610, location_BMRS_Final


def setup_update_B1610_data(location_BMRS_B1610: str, num_days: int = 14, hist_days: int = 45) -> pd.DataFrame:
    """
    Checks if the B1610 dataset exists or has been updated in the last n days (determined by "num_days").
    If not, it creates a new version of the dataset, using the "num_days" variable as the time limit for which
    to generate it.
    If it exists and has been updated recently, it finds the latest available date stored in the
    df_B1610 dataframe and updates only the missing recent data.

    Args:
        location_BMRS_B1610 (str): location of the B1610 parquet file
        num_days(int): max number of days for which to store the B1610 data. If the latest day in the B1610 dataset
                        is less recent than this timedelta, the function will simply request a new dataset.
        hist_days(int): maximum number of history days to keep.

    Returns:
        pd.DataFrame: dataframe with the updated B1610 (historical generation by BMU) data.
    """

    B1610_start_date = pd.to_datetime(
        date.today() - timedelta(days=num_days), utc=True
    )  # Default to 14 days ago to speed up API query
    B1610_end_date = pd.to_datetime(
        date.today() - timedelta(days=5), utc=True
    )  # The most recent B1610 data is ca. 5 days old

    if not os.path.isfile(os.path.join(location_BMRS_B1610, "B1610.parquet")):
        df_B1610 = client.get_B1610(B1610_start_date, B1610_end_date)
        df_B1610 = df_B1610.rename(columns={"bMUnitID": "bmUnitID"})

    else:
        df_B1610 = pd.read_parquet(os.path.join(location_BMRS_B1610, "B1610.parquet"))
        B1610_max_date = pd.to_datetime(df_B1610["settlementDate"], utc=True).max()
        B1610_update_start_date = pd.to_datetime(B1610_max_date + timedelta(days=1), utc=True)

        if B1610_max_date < B1610_start_date:
            df_B1610 = client.get_B1610(B1610_start_date, B1610_end_date)
            df_B1610 = df_B1610.rename(columns={"bMUnitID": "bmUnitID"})
        else:
            B1610_cutoff_date = pd.to_datetime(date.today() - timedelta(days=hist_days), utc=True)
            df_B1610["settlementDate"] = pd.to_datetime(df_B1610["settlementDate"], utc=True)
            df_B1610 = df_B1610.loc[df_B1610["settlementDate"] > B1610_cutoff_date]

            if B1610_update_start_date > B1610_max_date:
                df_B1610_append = client.get_B1610(B1610_update_start_date, B1610_end_date)
                df_B1610_append = df_B1610_append.rename(columns={"bMUnitID": "bmUnitID"})
                df_B1610 = pd.concat((df_B1610, df_B1610_append), axis=0)

    df_B1610 = df_B1610[["local_datetime", "settlementDate", "settlementPeriod", "bmUnitID", "quantity"]]

    df_B1610[["local_datetime", "settlementDate"]] = df_B1610[["local_datetime", "settlementDate"]].apply(
        pd.to_datetime, utc=True
    )
    df_B1610["settlementPeriod"] = df_B1610["settlementPeriod"].astype("int64")
    df_B1610["quantity"] = df_B1610["quantity"].astype("float64")

    df_B1610.to_parquet(os.path.join(location_BMRS_B1610, "B1610.parquet"))

    return df_B1610


def setup_update_PHYBM_data(BM_start_date: pd.Timestamp, location_BMRS_PHYBMDATA: str) -> pd.DataFrame:
    """
    Checks if the PHYBMDATA dataset exists. If not, it creates a new version of the dataset, using the
    last date on the df_B1610 dataset as the start date and the latest date as the end date.
    If it exists, it finds the latest available date stored in the df_B1610 dataframe, deletes any data now
    duplicated by the B1610 and fetches the lates PHYBMDATA.

    Once the B1610 data is updated, some balancing mechanism data will be redundant and can be removed.
    New data can be added.

    Args:
        BM_start_date (pd.Timestamp): Latest date in the B1610 dataframe plus one day.
                                        NB, the B1610 data always gets updated for entire days.
        location_BMRS_PHYBMDATA (str): location of the PHYBMDATA parquet file.

    Returns:
        pd.DataFrame: dataframe with the updated Physical BM Data.
    """
    BM_end_date = datetime.combine(date.today(), datetime.min.time()) + timedelta(days=1)

    if not os.path.isfile(os.path.join(location_BMRS_PHYBMDATA, "PHYBMDATA.parquet")):
        df_PHYBMDATA = client.get_PHYBMDATA(BM_start_date, BM_end_date)
        df_PHYBMDATA = df_PHYBMDATA.loc[df_PHYBMDATA["recordType"].isin(["PN", "MEL", "BOALF"])]

    else:
        df_PHYBMDATA = pd.read_parquet(os.path.join(location_BMRS_PHYBMDATA, "PHYBMDATA.parquet"))
        df_PHYBMDATA["settlementDate"] = pd.to_datetime(df_PHYBMDATA["settlementDate"]).dt.tz_localize(None)

        df_PHYBMDATA_start_date = (
            pd.to_datetime(df_PHYBMDATA["local_datetime"].max()) - timedelta(minutes=90)
        ).replace(
            tzinfo=None
        )  # NB, the FPN, BOAL and MEL could change/is not posted all at once. Hence, we want to also look at historic data.

        if df_PHYBMDATA_start_date < BM_start_date:
            # If the Physical BM Data hasn't been updated in a while, request a new dataset.
            df_PHYBMDATA = client.get_PHYBMDATA(BM_start_date, BM_end_date)
            df_PHYBMDATA = df_PHYBMDATA.loc[df_PHYBMDATA["recordType"].isin(["PN", "MEL", "BOALF"])]
        else:
            # Otherwise, only request the most recent data
            df_PHYBMDATA = df_PHYBMDATA.loc[df_PHYBMDATA["settlementDate"] >= BM_start_date]
            df_PHYBMDATA_latest = client.get_PHYBMDATA(df_PHYBMDATA_start_date, BM_end_date)
            df_PHYBMDATA_latest = df_PHYBMDATA_latest.loc[
                df_PHYBMDATA_latest["recordType"].isin(["PN", "MEL", "BOALF"])
            ]
            df_PHYBMDATA = pd.concat((df_PHYBMDATA, df_PHYBMDATA_latest), axis=0)

        df_PHYBMDATA = df_PHYBMDATA.drop_duplicates(keep="last")

    df_PHYBMDATA = df_PHYBMDATA[
        [
            "local_datetime",
            "recordType",
            "bmUnitID",
            "settlementDate",
            "settlementPeriod",
            "timeFrom",
            "pnLevelFrom",
            "timeTo",
            "pnLevelTo",
            "melLevelFrom",
            "melLevelTo",
            "bidOfferAcceptanceNumber",
            "acceptanceTime",
            "bidOfferLevelFrom",
            "bidOfferLevelTo",
        ]
    ]

    df_PHYBMDATA[["local_datetime", "settlementDate", "acceptanceTime", "timeFrom", "timeTo"]] = df_PHYBMDATA[
        ["local_datetime", "settlementDate", "acceptanceTime", "timeFrom", "timeTo"]
    ].apply(pd.to_datetime, utc=True)
    df_PHYBMDATA["settlementPeriod"] = df_PHYBMDATA["settlementPeriod"].astype("int64")
    df_PHYBMDATA[
        [
            "pnLevelFrom",
            "pnLevelTo",
            "melLevelFrom",
            "melLevelTo",
            "bidOfferAcceptanceNumber",
            "bidOfferLevelFrom",
            "bidOfferLevelTo",
        ]
    ] = df_PHYBMDATA[
        [
            "pnLevelFrom",
            "pnLevelTo",
            "melLevelFrom",
            "melLevelTo",
            "bidOfferAcceptanceNumber",
            "bidOfferLevelFrom",
            "bidOfferLevelTo",
        ]
    ].astype(
        "float64"
    )

    df_PHYBMDATA.to_parquet(os.path.join(location_BMRS_PHYBMDATA, "PHYBMDATA.parquet"))

    return df_PHYBMDATA


def filter_and_rename_physical_Data(df_PHYBMDATA: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the Physical BM data into the three record types that we're interested in:
    FPN, MEL and BOAL. Selects the relevant columns in each dataset to reduce the size of each DF.
    Renames the columns in the filtered DFs to follow a standard pattern.

    Args:
        df_PHYBMDATA (pd.DataFrame): current version of the PHYBMDATA dataframe.

    Returns:
        pd.DataFrame: Three dfs with the FPN, MEL and BOAL data respectively.
    """
    common_columns = [
        "local_datetime",
        "recordType",
        "bmUnitID",
        "settlementDate",
        "settlementPeriod",
        "timeFrom",
        "timeTo",
    ]
    fpn_columns = ["pnLevelFrom", "pnLevelTo"]
    mel_columns = ["melLevelFrom", "melLevelTo"]
    boal_columns = [
        "bidOfferAcceptanceNumber",
        "acceptanceTime",
        "bidOfferLevelFrom",
        "bidOfferLevelTo",
    ]

    df_fpn = df_PHYBMDATA.loc[df_PHYBMDATA["recordType"] == "PN", common_columns + fpn_columns]
    df_fpn = df_fpn.rename(columns={"pnLevelFrom": "LevelFrom", "pnLevelTo": "LevelTo"}).set_index("bmUnitID")

    df_mel = df_PHYBMDATA.loc[df_PHYBMDATA["recordType"] == "MEL", common_columns + mel_columns]
    df_mel = df_mel.rename(columns={"melLevelFrom": "LevelFrom", "melLevelTo": "LevelTo"}).set_index("bmUnitID")

    df_boal = df_PHYBMDATA.loc[df_PHYBMDATA["recordType"] == "BOALF", common_columns + boal_columns]
    df_boal["bidOfferAcceptanceNumber"] = df_boal["bidOfferAcceptanceNumber"].astype(int)
    df_boal = df_boal.rename(
        columns={
            "bidOfferLevelFrom": "LevelFrom",
            "bidOfferLevelTo": "LevelTo",
            "bidOfferAcceptanceNumber": "Accept ID",
        }
    ).set_index("bmUnitID")

    return df_fpn, df_mel, df_boal


def convert_physical_data_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all LevelFrom & timeFrom / LevelTo & timeTo columns from their horizontal format
    to a long format with values at different timepoints.

    Args:
        df (pd.DataFrame): BOAL, MEL or FPN dataframe to convert from wide to long.

    Returns:
        pd.DataFrame: BOAL, MEL or FPN dataframe with level/time to/from converted to long format.
    """

    df = pd.concat(
        (
            df.drop(columns=["LevelTo", "timeTo"]).rename(columns={"LevelFrom": "Level", "timeFrom": "Time"}),
            df.drop(columns=["LevelFrom", "timeFrom"]).rename(columns={"LevelTo": "Level", "timeTo": "Time"}),
        )
    )

    df["Level"] = df["Level"].astype(float)
    return df


def resolve_level(df_linear: pd.DataFrame, groupby: list) -> pd.DataFrame:
    """
    For BOAL data, we can have multiple levels for a given timepoint, because levels are fixed
    at one point and then overwitten at a later timepoint, before the moment in
    question has arrived.

    We need to resolve them, choosing the latest possible commitment for each timepoint. To achieve this,
    all data (FPN, MEL, BOAL) is first upsampled to 1-minutely resolution. This is easily possible because the data is
    recorded in MW (rater than MWh).

    Args:
        df (pd.DataFrame): BOAL, MEL or FPN dataframe to converted from wide to long.

    Returns:
        pd.DataFrame: BOAL, MEL or FPN dataframe data upsampled to 1-minutely resolution.
    """
    out = []
    for group_index, data in df_linear.groupby(groupby):
        high_freq = data.reset_index().rename(columns={"index": "Unit"}).set_index("Time").resample("T").first()
        out.append(high_freq.interpolate("ffill"))

    recombined = pd.concat(out)

    # Select the latest commitment for every timepoint
    resolved = recombined.reset_index().groupby(["Time", "bmUnitID"]).last()

    return resolved


def resolve_applied_bid_offer_level(df_linear: pd.DataFrame) -> pd.DataFrame:
    """
    BOAL Data is grouped by Accept ID and bmUnitID because the accept ID alone might not be unique.

    Args:
        df_linear (pd.DataFrame): BOAL dataframe to converted from wide to long.

    Returns:
        pd.DataFrame: BOAL data upsampled to the minutely level. Where multiple BOAL records exist for a time,
        only the last one is kept.

    """
    return resolve_level(df_linear, ["Accept ID", "bmUnitID"])


def resolve_FPN_MEL_level(df_linear: pd.DataFrame) -> pd.DataFrame:
    """
    FPN and MEL Data doesn't have an accept ID and only needs grouping by the bmUnitID.

    Args:
        df_linear (pd.DataFrame): MEL or FPN dataframe to converted from wide to long.

    Returns:
        pd.DataFrame: FPN/MEL data upsampled to the minutely level. Where multiple records exist for a time,
        only the last one is kept.
    """
    return resolve_level(df_linear, ["bmUnitID"])
