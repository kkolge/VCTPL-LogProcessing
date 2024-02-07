# imports 
import sys
import glob
import pandas as pd
# from datetime import datetime, timedelta
import PySimpleGUI as sg

import process_reader_log

# # Setting up the GUI Components
sg.theme('DarkAmber')

layout = [
    [sg.Text("Process Reader Logs - Raw logs")],  # = Row 1
    [sg.Text("Select Folder"), sg.In(size=(25, 1), enable_events=True, key="-FOLDER-"), sg.FolderBrowse()],
    [sg.ProgressBar(max_value=100, orientation="h", expand_x=True, size=(20, 20), key="-PBARFILE-")],
    [sg.Text(auto_size_text=True, key="-TEXTPROG-")],
    [sg.Button('Ok'), sg.Button('Exit')]
]

window = sg.Window("Reader log processor - Raw logs", layout, resizable=False)

# # processing the event loop
while True:
    event, value = window.read()
    if event == sg.WIN_CLOSED or event == "Exit":
        break
    if event == "-FOLDER-":
        folder_path = value["-FOLDER-"]
        print(f"folder_path: {folder_path}")
        res = glob.glob(f"{folder_path}/**/*.txt", recursive=True)
        # window["-PBARFILE-"].update(max_value=len(res))
        df_all_files = pd.DataFrame()
        bar_progress_counter = 1
        for file in res:
            print(f"Start importing file {file}")
            df_all_files = pd.concat([df_all_files, process_reader_log.load_file_to_df(file)],
                                     axis=0,
                                     ignore_index=True)
            print((bar_progress_counter * 100) // len(res))
            window["-PBARFILE-"].update(current_count=(bar_progress_counter * 100) // len(res))
            print(f"")
            bar_progress_counter += 1

        print(df_all_files.head())
        print(f"Number of cols x rows : {df_all_files.shape}")
        window["-TEXTPROG-"].update(f"Reading files completed.\nData shape: {df_all_files.shape}")

        # preprocessing and grouping
        df_all_files_preprocessed = process_reader_log.df_preprocess(df_all_files)
        window["-TEXTPROG-"].update(f"pre-processing data")
        df_all_files_preprocessed.to_csv(f"{folder_path}\combined.csv",
                          header=True,
                          sep=",",
                          index=False,
                          encoding="utf-8",
                          escapechar="\\")

        df_results = process_reader_log.get_results(df_all_files_preprocessed)
        window["-TEXTPROG-"].update(f"Generating results")

        # saving to outpuut file
        df_results.to_csv(f"{folder_path}\results.csv",
                          header=True,
                          sep=",",
                          index=False,
                          encoding="utf-8",
                          escapechar="\\")
        window["-TEXTPROG-"].update(f"Output file generated. filename: {folder_path}\results.csv")

window.close()

sys.exit(0)
