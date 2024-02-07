# imports 
import pandas as pd
from datetime import datetime, timedelta


# Reading the log file.
def load_file_to_df(filename):
    """This function takes a file name as an input and loads it into a dataframe
    and returns a dataframe"""
    try:
        df_file = pd.read_csv(filename,
                              sep=" : ",
                              header=None,
                              names=["Reader_IP", "Tag ID", "TEMP"], engine="python")
    except FileNotFoundError as fnfe:
        df_file = None
        print(f"{filename} not found. Please check the folder selection and try again")

    if df_file is not None:
        df_file.head()

    return df_file


def df_preprocess(df):
    # processing the dataframe to get the relevant data in the format that can be processed
    df[["RSSI", "Date", "Time", "AMPM"]] = df["TEMP"].str.split(" ", expand=True)
    df["TimeStamp"] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format="%m/%d/%Y %H:%M:%S")
    df = df.drop(labels=["Date", "Time", "AMPM", "TEMP"], axis=1)
    df.sort_values(by=["Tag ID", "Reader_IP", "TimeStamp"])
    df.reset_index()
    return df


def get_results(df):
    df["RSSI"] = df["RSSI"].astype(float)
    # df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    df.sort_values(by=["Tag ID", "Reader_IP", "TimeStamp"])
    # creating batches

    df['batch'] = ((df['Reader_IP'] != df['Reader_IP'].shift(1)) |
                   (df['Tag ID'] != df['Tag ID'].shift(1))) # |
                   # (df['TimeStamp'].shift(1) - df['TimeStamp'] > pd.Timedelta(1, 'h')))

    df['timestamp_int'] = df['TimeStamp'].astype(int)
    df['batch_number'] = (df['batch']).cumsum()

    # grouping the data by Tag ID and Reader IP
    df_grouped = df.groupby(["Tag ID", "Reader_IP", "batch_number"])

    # Aggregate function to get min and max time stamp and max rssi value in the group
    agg_functions = {
        'TimeStamp': ['min', 'max'],
        'RSSI': 'max'
    }

    result_df = df_grouped.agg(agg_functions).reset_index()

    return result_df


def temp():
    # processing the dataframe to get the relevant data in the format that can be processed
    # df[["RSSI", "Date", "Time", "AMPM"]] = df["TEMP"].str.split(" ", expand=True)
    # df["TimeStamp"] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format="%m/%d/%Y %H:%M:%S")
    # df = df.drop(labels=["Date", "Time", "AMPM", "TEMP"], axis=1)
    # df.reset_index()

    # df.head()

    # grouping the data by Tag ID
    df_group_tag_id = df.groupby(by="Tag ID", sort=True, group_keys=True)

    # this secript is not removing the relevant groups.
    # TODO: Fix this part
    rem_list = []
    for key in df_group_tag_id.groups.keys():
        if len(key) != 24:
            rem_list.append(key)
            # df_group_tag_id.groups.pop(key)

    [df_group_tag_id.groups.pop(key) for key in rem_list]

    # Now processing the data
    # Adding an outpur dataframe to store the processed data
    df_output = pd.DataFrame(columns=["Reader IP", "Tag ID", "Max RSSI", "Min Time", "Max Time", "Time Diff"])
    df_output["Reader IP"] = df_output["Reader IP"].astype(str)
    df_output["Tag ID"] = df_output["Tag ID"].astype(str)
    df_output["Max RSSI"] = df_output["Max RSSI"].astype(float)
    df_output["Min Time"] = df_output["Min Time"].astype('datetime64[ns]')
    df_output["Max Time"] = df_output["Max Time"].astype('datetime64[ns]')
    df_output["Time Diff"] = df_output["Time Diff"].apply(pd.to_timedelta(1, "s"))

    for key, group in df_group_tag_id:
        # initializing the variables for this group
        reader_ip = None
        max_rssi = None
        min_time = datetime.today() + timedelta(days=365)
        max_time = datetime.today() - timedelta(days=365)
        last_read_time = None
        subgroup_by_tag = group
        data_dict = {}

        subgroup_by_tag = subgroup_by_tag.sort_values(by="TimeStamp", ascending=True)
        for index, row in subgroup_by_tag.iterrows():
            if reader_ip == None:
                reader_ip = row["Reader_IP"]
            elif row["Reader_IP"] != reader_ip:
                print("Reader IP does not match")

            if max_rssi == None:
                max_rssi = row["RSSI"]
            elif row["RSSI"] > max_rssi:
                max_rssi = row["RSSI"]

            if last_read_time == None:
                last_read_time = row["TimeStamp"]
            elif row["TimeStamp"] - last_read_time > timedelta(hours=1):
                data_dict["Reader IP"] = reader_ip
                data_dict["Tag ID"] = key
                data_dict["Max RSSI"] = max_rssi
                data_dict["Min Time"] = min_time
                data_dict["Max Time"] = max_time
                data_dict["Time Diff"] = max_time - min_time
                df_output = pd.concat([df_output, pd.DataFrame(data_dict, index=[0])], ignore_index=True)
                df_output.reset_index()
                break

            if row["TimeStamp"] < min_time:
                min_time = row["TimeStamp"]

            if row["TimeStamp"] > max_time:
                max_time = row["TimeStamp"]

    # Writing data to output file
    df_output.to_csv(f"logs_output\{log_file_name}.out.csv", header=True, sep=",", index=False, encoding="utf-8")
    print(f"Processed {log_file_name}")
