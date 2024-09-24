import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def time_to_float(time_value):
    # Check for NaN or None
    if pd.isna(time_value):
        return np.nan
    
    # Convert datetime objects to string in 'h:m:s AM/PM' format
    if isinstance(time_value, (datetime, pd.Timestamp)):
        # Convert to string in the appropriate format
        time_value = time_value.strftime('%I:%M:%S %p')
    # If input is already a datetime.time object, convert to float
    elif isinstance(time_value, time):
        return time_value.hour + time_value.minute / 60.0 + time_value.second / 3600.0
    
    # Check for string inputs and parse them into datetime objects
    if isinstance(time_value, str):
        try:
            # Normalize time strings without space between time and AM/PM
            if 'AM' in time_value or 'PM' in time_value:
                time_value = time_value.replace('AM', ' AM').replace('PM', ' PM')
            # Check if the string has seconds by counting the ':' characters
            if time_value.count(':') == 2:  # Handles formats with seconds like '12:00:00 AM'
                parsed_time = pd.to_datetime(time_value, format='%I:%M:%S %p')
            elif time_value.count(':') == 1:  # Handles formats without seconds like '12:00 AM'
                parsed_time = pd.to_datetime(time_value, format='%I:%M %p')
            else:  # Handles formats with only hour '12 PM'
                parsed_time = pd.to_datetime(time_value, format='%I %p')
            return parsed_time.hour + parsed_time.minute / 60.0
        except ValueError:
            return np.nan
        
    return np.nan

def float_to_time(h):
    if pd.isna(h):  # Check for NaN
        return None
    try:
        time_delta = timedelta(hours=h)
        base_time = datetime(1, 1, 1) + time_delta
        time_str = base_time.strftime('%I:%M %p')
        return time_str
    except:
        return None
    

def get_string(df):
    # Convert all values to strings where applicable, handle None values
    string = []
    for value in df:
        if pd.isna(value):  # Check for NaN and append NaN
            string.append(np.nan)
        elif isinstance(value, str):  # If value is already a string, split it
            split_value = value.split(' ')
            string.append(split_value[0] if len(split_value) > 0 else None)
        else:  # Convert non-string types to string and then split
            str_value = str(value)
            split_value = str_value.split(' ')
            string.append(split_value[0] if len(split_value) > 0 else None)

    return string    
    
# Adjusting the function to handle both cases correctly as described
def data_conversion(main_df, main_columns, new_columns, codes, subjobs, dept, a_list, t_list):
    df = main_df.copy()
    m_df = main_df.copy()
    n_approval_df = df[df['Status'].isin(['pending', 'reviewed'])].copy()
    mapping = dict(zip(codes['Name'].astype(str), codes['Number']))
    mapping_subjobs = dict(zip(subjobs['Job Num'].astype(str), subjobs['Code'].astype(str)))
    mapping_dept = dict(zip(dept['Job'].astype(str), dept['Code'].astype(str)))

    # Process the 'Filtered' column for cost code mapping
    m_df['Filtered'] = m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][11])]].apply(
        lambda x: x.split('- ')[-1] if '-' in str(x) else x)

    # Identify and flag entries with duplicates for the same person on the same day
    m_df['DuplicateFlag'] = m_df.duplicated(
        subset=[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][0])], 
                m_df.columns[m_df.columns.get_loc(main_columns['Columns'][1])]],  
        keep=False
    )

    m_df['IsPerDiem950'] = np.where(
        (m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][7])]] == 'Per-diem') &  
        (m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][2])]] == '1-950'), 
        True,
        False
    )
    
    m_df.drop(['DuplicateFlag', 'IsPerDiem950'], axis=1, inplace=True)

    df = pd.DataFrame({
        new_columns['Columns'][0]: m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][0])]],

        new_columns['Columns'][1]: np.where(
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][0])]].isin(codes['Name']), 
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][0])]].map(mapping), 
            None),

        new_columns['Columns'][2]: m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][1])]],
        new_columns['Columns'][3]: m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][2])]],
        new_columns['Columns'][4]: None,
        new_columns['Columns'][5]: np.where(
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][3])]].isin(mapping_subjobs),
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][3])]].map(mapping_subjobs),
            np.where(
                m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][4])]].isin(mapping_dept),
                m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][4])]].map(mapping_dept),
                np.where(
                    m_df['Filtered'].isin(mapping_dept),
                    m_df['Filtered'].map(mapping_dept),
                    np.where(
                        m_df['Filtered'].isin(a_list['Job']),
                        'CON-CLOCK',
                        np.where(
                            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][5])]].isin(mapping_subjobs),
                            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][5])]].map(mapping_subjobs),
                            None
                        )
                    )
                )
            )
        ),
        new_columns['Columns'][6]: np.where(
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][2])]].isin(a_list['Job']),
            'CON-CLOCK', 
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][3])]]
        ),
        new_columns['Columns'][7]: None,
        new_columns['Columns'][8]: None,
        new_columns['Columns'][9]: m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][6])]],
        new_columns['Columns'][10]: np.where(
            m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][7])]] == 'Per-diem', 
            -55, 
            None
        ),
        new_columns['Columns'][11]: m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][8])]]
    })

    df[new_columns['Columns'][3]] = df[new_columns['Columns'][3]].apply(
        lambda x: f"1-{x}" if isinstance(x, str) and x.upper() not in map(str.upper, a_list['Job']) and '1-' not in x else x
    )
    df[new_columns['Columns'][6]] = df[new_columns['Columns'][6]].fillna(m_df[main_columns['Columns'][5]].reindex(df.index))

    # fix columns
    df[new_columns['Columns'][4]] = df[new_columns['Columns'][4]].fillna('LA')
    df[new_columns['Columns'][4]] = np.where(
        df[new_columns['Columns'][10]] == -55,
        df[new_columns['Columns'][4]] + '|' + 'PD',
        df[new_columns['Columns'][4]]
    )

    df['new'] = df[new_columns['Columns'][11]]
    df[new_columns['Columns'][11]] = get_string(df[new_columns['Columns'][11]])

    df[new_columns['Columns'][4]] = np.where(
        df[new_columns['Columns'][11]].isin(t_list['Name']),
        df[new_columns['Columns'][4]] + '|' + 'MI',
        df[new_columns['Columns'][4]]
    )
    df[new_columns['Columns'][11]] = df['new']
    df.drop('new', axis=1, inplace=True)

    # Convert 'Start Time' and 'Stop Time' to floats representing hours
    try:
        df[new_columns["Columns"][7]] = m_df[main_columns['Columns'][9]].apply(time_to_float)
        df[new_columns["Columns"][8]] = m_df[main_columns['Columns'][10]].apply(time_to_float)
    except Exception as e:
        print(f"Error converting time to float: {e}")
        pass

    # Handle missing times
    mask_start_none = df[new_columns["Columns"][7]].isna() & df[new_columns["Columns"][8]].notna()
    df.loc[mask_start_none, new_columns["Columns"][7]] = df.loc[mask_start_none, new_columns["Columns"][8]] - df.loc[mask_start_none, new_columns["Columns"][9]]

    mask_stop_none = df[new_columns["Columns"][8]].isna() & df[new_columns["Columns"][7]].notna()
    df.loc[mask_stop_none, new_columns["Columns"][8]] = df.loc[mask_stop_none, new_columns["Columns"][7]] + df.loc[mask_stop_none, new_columns["Columns"][9]]

    mask_both_none = df[new_columns["Columns"][7]].isna() & df[new_columns["Columns"][8]].isna()
    df.loc[mask_both_none, new_columns["Columns"][7]] = 6.0  # Start at midnight
    df.loc[mask_both_none, new_columns["Columns"][8]] = df.loc[mask_both_none, new_columns["Columns"][7]] + df.loc[mask_both_none, new_columns["Columns"][9]]

    # Convert float hours back to time strings
    df[new_columns["Columns"][7]] = df[new_columns["Columns"][7]].apply(float_to_time)
    df[new_columns["Columns"][8]] = df[new_columns["Columns"][8]].apply(float_to_time)

    # Temporary conversion of the 'Date' column to datetime to combine with times
    temp_dates = pd.to_datetime(df[new_columns["Columns"][2]], format='%m/%d/%Y')  # Explicit date format

    # Combine 'Date' and 'Start/Stop Time' to create datetime objects for internal processing
    df[new_columns["Columns"][7]] = pd.to_datetime(temp_dates.astype(str) + ' ' + df[new_columns["Columns"][7]], format='%Y-%m-%d %I:%M %p')
    df[new_columns["Columns"][8]] = pd.to_datetime(temp_dates.astype(str) + ' ' + df[new_columns["Columns"][8]], format='%Y-%m-%d %I:%M %p')

    # Sort by employee, date, and start time
    df.sort_values(by=[new_columns["Columns"][0], new_columns["Columns"][2], new_columns["Columns"][7]], inplace=True)

    # Loop through and adjust overlapping times for the same person on the same day
    for i in range(1, len(df)):
        # Check if it's the same person on the same day
        if df.loc[df.index[i], new_columns["Columns"][0]] == df.loc[df.index[i-1], new_columns["Columns"][0]] and \
        df.loc[df.index[i], new_columns["Columns"][2]] == df.loc[df.index[i-1], new_columns["Columns"][2]]:
            
            # Check if the start time overlaps with the previous stop time
            if df.loc[df.index[i], new_columns["Columns"][7]] <= df.loc[df.index[i-1], new_columns["Columns"][8]]:
                # Set the start time to one minute after the previous stop time
                df.loc[df.index[i], new_columns["Columns"][7]] = df.loc[df.index[i-1], new_columns["Columns"][8]] + timedelta(minutes=4)
                # Adjust the stop time based on the new start time and total time
                df.loc[df.index[i], new_columns["Columns"][8]] = df.loc[df.index[i], new_columns["Columns"][7]] + timedelta(hours=df.loc[df.index[i], new_columns["Columns"][9]])

    # Convert 'Start Time' and 'Stop Time' back to the desired string format
    df[new_columns["Columns"][7]] = df[new_columns["Columns"][7]].dt.strftime('%I:%M %p')
    df[new_columns["Columns"][8]] = df[new_columns["Columns"][8]].dt.strftime('%I:%M %p')

    # Revert the 'Date' column back to its original format (MM/DD/YYYY)
    df[new_columns["Columns"][2]] = temp_dates.dt.strftime('%m/%d/%Y')

    df[new_columns['Columns'][4]] = df[new_columns['Columns'][4]].str.split('|')
    df = df.explode(new_columns["Columns"][4]).reset_index(drop=True)

    df[new_columns["Columns"][10]] = np.where(
        df[new_columns['Columns'][4]].isin(['LA', 'MI']),
        None,
        df[new_columns["Columns"][10]]
    )

    df[new_columns["Columns"][11]] = np.where(
        df[new_columns['Columns'][4]].isin(['LA', 'PD']),
        None,
        df[new_columns["Columns"][11]]
    )

    df[new_columns["Columns"][3]] = np.where(
        df[new_columns["Columns"][3]] == 'TRAINING',
        "TRAIN",
        df[new_columns["Columns"][3]]
    )

    df[new_columns["Columns"][9]] = np.where(
        df[new_columns["Columns"][4]].isin(['PD', 'MI']),
        None,
        df[new_columns["Columns"][9]]
    )

    df[new_columns["Columns"][7]] = np.where(
        df[new_columns["Columns"][4]].isin(['PD', 'MI']),
        None,
        df[new_columns["Columns"][7]]
    )

    df[new_columns["Columns"][8]] = np.where(
        df[new_columns["Columns"][4]].isin(['PD', 'MI']),
        None,
        df[new_columns["Columns"][8]]
    )

    # Remap for missing values in 
    df[new_columns["Columns"][6]] = df[new_columns["Columns"][6]].astype(str)  
    
    df[new_columns["Columns"][5]] = df[new_columns["Columns"][5]].fillna(
        df[new_columns["Columns"][6]].map(mapping_dept)
    )

    df[new_columns["Columns"][3]] = np.where(
        df[new_columns["Columns"][4]].isin(['PD', 'MI']),
        '1-950',
        df[new_columns["Columns"][3]]
    )
    df = df[~((df[new_columns['Columns'][9]] == 0) & (df[new_columns['Columns'][4]] == 'LA'))]

    return df[df[new_columns['Columns'][0]].notna() & (df[new_columns['Columns'][0]] != '')], n_approval_df


