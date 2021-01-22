import json
import os
import pandas as pd
import copy



def load_metadata_from_raw_data(data_path, status):
    chronological_events_order = pd.DataFrame(columns=["index", "ID", "date", "event", "status", 'file_name'])
    for status in status:
        print("loading data for status : ", status)
        for file_name in os.listdir(os.path.join(data_path, status, 'data')):
            index = int(file_name.split("_")[0])
            changes = json.load(open(os.path.join(data_path, status, 'data', file_name)))
            for local_index, change in enumerate(changes):
                chrono_row1 = {}
                chrono_row2 = {}

                chrono_row1["index"] = local_index
                chrono_row1["ID"] = change["id"]
                chrono_row1["status"] = status
                chrono_row1["file_name"] = file_name
                chrono_row1["date"] = change["created"]
                chrono_row1["event"] = 'create'
                chrono_row2 = copy.deepcopy(chrono_row1)
                chrono_row2["date"] = change["updated"]
                chrono_row2["event"] = 'close'
                chronological_events_order = chronological_events_order.append(chrono_row1, ignore_index=True)
                chronological_events_order = chronological_events_order.append(chrono_row2, ignore_index=True)

    # converting dates to datetime
    chronological_events_order["date"] = pd.to_datetime(chronological_events_order["date"])
    return chronological_events_order


def preprocess_metadata(metadata):
    # removing duplicates
    metadata = metadata.drop_duplicates(subset=['ID', 'event'])
    # sorting data according to update date
    sorted_metadata = metadata.sort_values(by=['date'])
    return sorted_metadata


def save_metadata(metadata, name):
    metadata.to_excel(name, index=False)


def load_metadata(path):
    return pd.read_excel(os.path.join(path, "meta_data.xlsx"))





#data = load_metadata_from_raw_data(data_path='C:/Users/AQ38570/Desktop/work/mcr_feature_extractor/libreoffice',
#                                  status=['abondaned', 'merged'])
#preprocessed_metadata = preprocess_metadata(data)
#save_metadata(preprocessed_metadata, 'C:/Users/AQ38570/Desktop/work/mcr_feature_extractor/libreoffice_metadata.xlsx')