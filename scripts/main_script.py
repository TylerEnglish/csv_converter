import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def time_to_float(time):
    # Check for NaN or None
    if pd.isna(time):
        return np.nan
    # Check for string inputs and parse them into datetime objects
    if isinstance(time, str):
        try:
            parsed_time = pd.to_datetime(time, format='%I:%M %p')
            return parsed_time.hour + parsed_time.minute / 60.0
        except ValueError:
            return np.nan
    # If input is already a datetime object, convert to float
    elif isinstance(time, (datetime, pd.Timestamp)):
        return time.hour + time.minute / 60.0
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
    array = df.str.split(' ')
    string = []

    for a in array:
        if type(a) is list:
            string.append(a[0])
        else:
            string.append(a)
    
    return string
    

    
# Adjusting the function to handle both cases correctly as described
def data_conversion(main_df, main_columns, new_columns, codes, subjobs, dept, a_list, t_list):
    df = main_df.copy()
    m_df = df[~df['Status'].isin(['pending', 'reviewed'])].copy()
    n_approval_df = df[df['Status'].isin(['pending', 'reviewed'])].copy()=
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
    
    def clear_per_diem(row):
        if row['DuplicateFlag'] and not row['IsPerDiem950']:
            return None 
        return row[main_columns['Columns'][7]] 
    m_df[m_df.columns[m_df.columns.get_loc(main_columns['Columns'][7])]] = m_df.apply(clear_per_diem, axis=1)
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

    try:
        df[new_columns["Columns"][7]] = m_df[main_columns['Columns'][9]].apply(time_to_float)
        df[new_columns["Columns"][8]] = m_df[main_columns['Columns'][10]].apply(time_to_float)
    except:
        pass


    # Handle missing times=
    mask_start_none = df[new_columns["Columns"][7]].isna() & df[new_columns["Columns"][8]].notna()
    df.loc[mask_start_none, new_columns["Columns"][7]] = df.loc[mask_start_none, new_columns["Columns"][8]] - df.loc[mask_start_none, new_columns["Columns"][9]]

    mask_stop_none = df[new_columns["Columns"][8]].isna() & df[new_columns["Columns"][7]].notna()
    df.loc[mask_stop_none, new_columns["Columns"][8]] = df.loc[mask_stop_none, new_columns["Columns"][7]] + df.loc[mask_stop_none, new_columns["Columns"][9]]

    mask_both_none = df[new_columns["Columns"][7]].isna() & df[new_columns["Columns"][8]].isna()
    df.loc[mask_both_none, new_columns["Columns"][7]] = 6.0  
    df.loc[mask_both_none, new_columns["Columns"][8]] = df.loc[mask_both_none, new_columns["Columns"][7]] + df.loc[mask_both_none, new_columns["Columns"][9]]

    df[new_columns["Columns"][7]] = df[new_columns["Columns"][7]].apply(float_to_time)
    df[new_columns["Columns"][8]] = df[new_columns["Columns"][8]].apply(float_to_time)

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


