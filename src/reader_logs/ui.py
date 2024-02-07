# imports 
import sys
import os.path
import csv

import PySimpleGUI as sg

import process_reader_log

# # Setting up the GUI Components
sg.theme('DarkAmber')

layout = [
    [sg.Text("Process Reader Logs - Raw logs")],  # = Row 1
    [sg.Text("Select Folder"),
     sg.In(size=(25, 1),
           enable_events=True,
           key="-FOLDER-", disabled=True, text_color="black"),
     sg.FolderBrowse()],
    [sg.Multiline(size=(50, 10),
                  key="-TEXTPROG-",
                  autoscroll=True,
                  auto_refresh=True,
                  reroute_stdout=True,
                  default_text="VCTPL Reader Log Processing")],
    [sg.Button('Exit')]
]

window = sg.Window("VCTPL Reader log processor - Raw logs",
                   layout,
                   resizable=False,
                   finalize=True)

# # processing the event loop
while True:
    event, value = window.read()
    if event == sg.WIN_CLOSED or event == "Exit":
        break
    if event == "-FOLDER-":
        window.set_cursor(cursor="exchange")
        folder_path = value["-FOLDER-"]
        print(f"folder_path: {folder_path}")
        # res = glob.glob(f"{folder_path}/**/*.txt", recursive=True)
        # window["-PBARFILE-"].update(max_value=len(res))
        # df_all_files = pd.DataFrame()
        # bar_progress_counter = 1
        # for file in res:
        #     # print(f"Start importing file {file}")
        #     df_all_files = pd.concat([df_all_files, process_reader_log.load_file_to_df(file)],
        #                              axis=0,
        #                              ignore_index=True)
        #     print((bar_progress_counter * 100) // len(res))
        #     window["-PBARFILE-"].update(current_count=(bar_progress_counter * 100) // len(res))
        #     print(f"")
        #     bar_progress_counter += 1

        df_all_files = process_reader_log.load_file_to_df(folder_path)
        # print(df_all_files.head())
        print(f"Reading files completed.")
        print(f"Number of rows x cols : {df_all_files.shape}")

        # preprocessing and grouping
        df_all_files_preprocessed = process_reader_log.df_preprocess(df_all_files)
        print(f"pre-processing data")
        # df_all_files_preprocessed.to_csv(f"{folder_path}\combined.csv",
        #                   header=True,
        #                   sep=",",
        #                   index=False,
        #                   encoding="utf-8",
        #                   escapechar="\\")

        df_results = process_reader_log.process2(df_all_files_preprocessed)
        print(f"Generating output file")

        # saving to output file
        file_path = os.path.join(folder_path, "results.csv")
        fields = ["Tag ID", "Reader IP", "Min Timestamp", "Max Timestamp", "Max RSSI"]
        with open(file_path, 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f, delimiter=',',
                               quotechar='|', quoting=csv.QUOTE_MINIMAL, escapechar="\\")
            write.writerow(fields)
            write.writerows(df_results)

        print(f"Output file generated. filename: {file_path}")
        window.set_cursor("arrow")

window.close()

sys.exit(0)
