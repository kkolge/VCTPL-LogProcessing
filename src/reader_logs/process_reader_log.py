# imports 
import glob

import pandas as pd
from datetime import datetime, timedelta

# constant use to separate multiple trips
TIME_GAP = timedelta(minutes=30)


# Reading the log file.
def load_file_to_df(folder_name):
    """This function takes a folder name as an input and loads all the data from all logfiles into a dataframe
    and returns a dataframe"""
    res = glob.glob(f"{folder_name}/**/*.txt", recursive=True)
    df_all_files = pd.DataFrame()
    for file in res:
        print(f"Start importing file {file}")
        df_file = pd.read_csv(file,
                              sep=" : ",
                              header=None,
                              names=["Reader_IP", "Tag ID", "TEMP"], engine="python")
        df_all_files = pd.concat([df_all_files, df_file],
                                 axis=0,
                                 ignore_index=True)

    return df_all_files


def process(df):
    """
        Following steps will be performed to process the data
        1. initialize a list to hold the final set of rows
        2. initialize paramerets that need to follow the loop for comparision
        2.1 tag_id
        2.2 groups
        3. read a row
        4. check if the tag id is same as last row. if not, create a new group
        5.
    """
    tag_id = None
    reader_id = None
    max_rssi = None
    min_timestamp = None
    max_timestamp = None
    tag_reader_group = []

    for index, row in df.iterrows():
        # Handling first row
        if row["Tag ID"] is None and row["Reader_IP"] is None:
            tag_id = row["Tag ID"]
            reader_id = row["Reader_IP"]
            max_rssi = 0.0
            min_timestamp = row["TimeStamp"]
            max_timestamp = row["TimeStamp"]
            continue

        # Handling other rows
        if row["Tag ID"] == tag_id:
            if row["Reader_IP"] == reader_id:
                if row["TimeStamp"] < min_timestamp:
                    min_timestamp = row["TimeStamp"]
                if row["TimeStamp"] > max_timestamp:
                    if row["TimeStamp"] - max_timestamp > TIME_GAP:
                        tag_reader_group.append([tag_id, reader_id, min_timestamp, max_timestamp, max_rssi])
                        tag_id = row["Tag ID"]
                        reader_id = row["Reader_IP"]
                        max_rssi = 0.0
                        min_timestamp = row["TimeStamp"]
                        max_timestamp = row["TimeStamp"]
                    else:
                        max_timestamp = row["TimeStamp"]
                if row["RSSI"] > max_rssi:
                    max_rssi = row["RSSI"]
            else:
                tag_reader_group.append([tag_id, reader_id, min_timestamp, max_timestamp, max_rssi])
                reader_id = row["Reader_IP"]
                max_rssi = 0.0
                min_timestamp = row["TimeStamp"]
                max_timestamp = row["TimeStamp"]
        else:
            tag_reader_group.append([tag_id, reader_id, min_timestamp, max_timestamp, max_rssi])
            tag_id = row["Tag ID"]
            reader_id = row["Reader_IP"]
            max_rssi = 0.0
            min_timestamp = row["TimeStamp"]
            max_timestamp = row["TimeStamp"]

    print(f"Total unique rows found: {len(tag_reader_group)}")
    return tag_reader_group


def process2(df):
    """
        Following steps will be performed to process the data
        1. initialize a list to hold the final set of rows
        2. initialize paramerets that need to follow the loop for comparision
        2.1 tag_id
        2.2 groups
        3. read a row
        4. check if the tag id is same as last row. if not, create a new group
        5.
    """
    tag_id = None
    reader_id = None
    max_rssi = None
    min_timestamp = None
    max_timestamp = None
    tag_reader_group = []

    for index, row in df.iterrows():
        # Handling first row
        if row["Tag ID"] is None and row["Reader_IP"] is None:
            tag_id = row["Tag ID"]
            reader_id = row["Reader_IP"]
            max_rssi = 0.0
            min_timestamp = row["TimeStamp"]
            max_timestamp = row["TimeStamp"]
            continue

        # Handling other rows
        if row["Tag ID"] == tag_id:
            if row["TimeStamp"] < min_timestamp:
                min_timestamp = row["TimeStamp"]
            if row["TimeStamp"] > max_timestamp:
                if row["TimeStamp"] - max_timestamp > TIME_GAP:
                    tag_reader_group.append([tag_id, reader_id, min_timestamp, max_timestamp, max_rssi])
                    tag_id = row["Tag ID"]
                    reader_id = row["Reader_IP"]
                    max_rssi = 0.0
                    min_timestamp = row["TimeStamp"]
                    max_timestamp = row["TimeStamp"]
                else:
                    max_timestamp = row["TimeStamp"]
            if row["RSSI"] > max_rssi:
                max_rssi = row["RSSI"]
        else:
            tag_reader_group.append([tag_id, reader_id, min_timestamp, max_timestamp, max_rssi])
            tag_id = row["Tag ID"]
            reader_id = row["Reader_IP"]
            max_rssi = 0.0
            min_timestamp = row["TimeStamp"]
            max_timestamp = row["TimeStamp"]

    print(f"Total unique rows found: {len(tag_reader_group)}")
    return tag_reader_group


def df_preprocess(df):
    # processing the dataframe to get the relevant data in the format that can be processed
    df[["RSSI", "Date", "Time", "AMPM"]] = df["TEMP"].str.split(" ", expand=True)
    df["TimeStamp"] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format="%m/%d/%Y %H:%M:%S")
    df = df.drop(labels=["Date", "Time", "AMPM", "TEMP"], axis=1)
    df["RSSI"] = df["RSSI"].astype(float)
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    df = df.sort_values(by=["Tag ID", "Reader_IP", "TimeStamp"])
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
