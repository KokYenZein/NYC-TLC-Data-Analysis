import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df = df.drop_duplicates().reset_index(drop=True)
    df["TripID"] = df.index

    
    DateTime = df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]].reset_index(drop=True)
    DateTime["pickup_hour"] = DateTime["tpep_pickup_datetime"].dt.hour
    DateTime["pickup_day"] = DateTime["tpep_pickup_datetime"].dt.day
    DateTime["pickup_month"] = DateTime["tpep_pickup_datetime"].dt.month
    DateTime["pickup_year"] = DateTime["tpep_pickup_datetime"].dt.year
    DateTime["pickup_weekday"] = DateTime["tpep_pickup_datetime"].dt.weekday
    DateTime["dropoff_hour"] = DateTime["tpep_dropoff_datetime"].dt.hour
    DateTime["dropoff_day"] = DateTime["tpep_dropoff_datetime"].dt.day
    DateTime["dropoff_month"] = DateTime["tpep_dropoff_datetime"].dt.month
    DateTime["dropoff_year"] = DateTime["tpep_dropoff_datetime"].dt.year
    DateTime["dropoff_weekday"] = DateTime["tpep_dropoff_datetime"].dt.weekday 

    DateTime["DateTime_ID"] = DateTime.index
    DateTime = DateTime[["DateTime_ID", 
          "tpep_pickup_datetime", "pickup_hour", "pickup_day", "pickup_month", "pickup_year", "pickup_weekday", 
          "tpep_dropoff_datetime", "dropoff_hour", "dropoff_day", "dropoff_month", "dropoff_year", "dropoff_weekday"]]

    PassengerCount = df[["passenger_count"]].reset_index(drop=True)
    PassengerCount["PassengerCount_ID"] = PassengerCount.index
    PassengerCount = PassengerCount[["PassengerCount_ID", "passenger_count"]]

    TripDistance = df[["trip_distance"]].reset_index(drop=True)
    TripDistance["TripDistance_ID"] = TripDistance.index
    TripDistance = TripDistance[["TripDistance_ID", "trip_distance"]]

    PickupLocation = df[["pickup_longitude", "pickup_latitude"]].reset_index(drop=True)
    PickupLocation["PickupLocation_ID"] = PickupLocation.index
    PickupLocation = PickupLocation[["PickupLocation_ID", "pickup_longitude", "pickup_latitude"]]

    DropoffLocation = df[["dropoff_longitude", "dropoff_latitude"]].reset_index(drop=True)
    DropoffLocation["DropoffLocation_ID"] = DropoffLocation.index
    DropoffLocation = DropoffLocation[["DropoffLocation_ID", "dropoff_longitude", "dropoff_latitude"]]

    RateCode_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    RateCode = df[["RatecodeID"]].reset_index(drop=True)
    RateCode["RateCode_ID"] = RateCode.index
    RateCode["Ratecode_name"] = RateCode["RatecodeID"].map(RateCode_type)
    RateCode = RateCode[["RateCode_ID", "RatecodeID", "Ratecode_name"]]

    PaymentType_type = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    PaymentType = df[["payment_type"]].reset_index(drop=False)
    PaymentType["PaymentType_ID"] = PaymentType.index
    PaymentType["payment_type_name"] = PaymentType["payment_type"].map(PaymentType_type)
    PaymentType = PaymentType[["PaymentType_ID", "payment_type", "payment_type_name"]]

    FactTable = df.merge(DateTime, left_on="TripID", right_on="DateTime_ID") \
            .merge(PassengerCount, left_on="TripID", right_on="PassengerCount_ID") \
            .merge(TripDistance, left_on="TripID", right_on="TripDistance_ID") \
            .merge(PickupLocation, left_on="TripID", right_on="PickupLocation_ID") \
            .merge(DropoffLocation, left_on="TripID", right_on="DropoffLocation_ID") \
            .merge(RateCode, left_on="TripID", right_on="RateCode_ID") \
            .merge(PaymentType, left_on="TripID", right_on="PaymentType_ID") \
            [["TripID", "VendorID", "DateTime_ID", "PassengerCount_ID", "TripDistance_ID",
             "RateCode_ID", "store_and_fwd_flag", "PickupLocation_ID", "DropoffLocation_ID", "PaymentType_ID",
             "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount"]]
    
    return {"DateTime":DateTime.to_dict(orient="dict"),
    "PassengerCount":PassengerCount.to_dict(orient="dict"),
    "TripDistance":TripDistance.to_dict(orient="dict"),
    "PickupLocation":PickupLocation.to_dict(orient="dict"),
    "DropoffLocation":DropoffLocation.to_dict(orient="dict"),
    "RateCode":RateCode.to_dict(orient="dict"),
    "PaymentType":PaymentType.to_dict(orient="dict"),
    "FactTable":FactTable.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
