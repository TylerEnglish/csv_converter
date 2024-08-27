import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def float_to_time(h):
    try:
        time_delta = timedelta(hours=h)
        base_time = datetime(1,1,1) + time_delta

        time_str = base_time.strftime('%I:%M %p')
        return time_str
    except:
        None

    return h
    
def time_to_float(time) -> float:
    return time.dt.hour

def get_string(df):
    array = df.str.split(' ')
    string = []

    for a in array:
        if type(a) is list:
            string.append(a[0])
        else:
            string.append(a)
    
    return string
    

    
def data_conversion(main_df, main_columns, new_columns, codes, subjobs, dept, a_list, t_list):
    # Some constants
    m_df = main_df.copy()
    codes = codes.copy()
    subjobs = subjobs.copy()
    dept = dept.copy()
    ad_list = a_list.copy()
    t_list = t_list.copy()
    new_c = new_columns.copy()
    main_c = main_columns.copy()
  
    # Fixed values
    mapping = dict(zip(codes['Name'],codes['Number']))
    mapping_subjobs = dict(zip(subjobs['Job Num'], subjobs['Code']))
    mapping_dept = dict(zip(dept['Job'], dept['Code']))
    m_df['Filtered'] = m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][11])]].apply(lambda x: x.split('- ')[-1] if '-' in str(x) else x)

    m_df['DuplicateFlag'] = m_df.duplicated(subset=[m_df.columns[m_df.columns.get_loc(main_c['Columns'][0])], m_df.columns[m_df.columns.get_loc(main_c['Columns'][1])]], keep=False)

    # Step 2: Filter relevant entries (Cost Code '1-950' and 'Per-diem')
    m_df['IsPerDiem950'] = np.where(
        (m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][7])]] == 'Per-diem') & 
        (m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][2])]] == '1-950'), True, False
    )

    # Step 3: Clean up other Per-diem entries for the same person on the same day
    def clear_per_diem(row):
        if row['DuplicateFlag'] and not row['IsPerDiem950']:
            return None  # Remove Per-diem
        return row[new_c['Columns'][10]]  # Keep original value if conditions aren't met
    
    m_df[new_c['Columns'][10]] = m_df.apply(clear_per_diem, axis=1)
    
    # Drop the helper columns
    m_df.drop(['DuplicateFlag', 'IsPerDiem950'], axis=1, inplace=True)
    # Create DataFrame
    df = pd.DataFrame({
        new_c['Columns'][0]: m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][0])]],

        new_c['Columns'][1]: np.where(
                              m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][0])]].isin(codes['Name']), 
                              m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][0])]].map(mapping), 
                              None),

        new_c['Columns'][2]: m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][1])]],
        new_c['Columns'][3]: m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][2])]],
        new_c['Columns'][4]: None,
        new_c['Columns'][5]: np.where(
                m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][3])]].isin(mapping_subjobs),
                m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][3])]].map(mapping_subjobs),
                np.where(
                    m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][4])]].isin(mapping_dept),
                    m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][4])]].map(mapping_dept),
                    np.where(
                        m_df['Filtered'].isin(mapping_dept),
                        m_df['Filtered'].map(mapping_dept),
                        np.where(
                            m_df['Filtered'].isin(ad_list['Job']),
                            'CON-CLOCK',
                                   np.where(
                                      m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][5])]].isin(mapping_subjobs),
                                      m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][5])]].map(mapping_subjobs),
                                      None
                                   )
                        )
                    )
                )
        ),

        new_c['Columns'][6]: np.where(m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][2])]].isin(ad_list['Job']),
                                        'CON-CLOCK', m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][3])]]),

        new_c['Columns'][7]: None,
        new_c['Columns'][8]: None,
        new_c['Columns'][9]: m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][6])]],
        new_c['Columns'][10]: np.where(m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][7])]] == 'Per-diem', -55, None),
        new_c['Columns'][11]: m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][8])]]
    })

    # Fill in more of the values
    df[new_c['Columns'][3]] = df[new_c['Columns'][3]].apply(
        lambda x: f"1-{x}" if isinstance(x, str) and x.upper() not in map(str.upper, ad_list['Job']) and '1-' not in x else x
    )
    df[new_c['Columns'][6]] = df[new_c['Columns'][6]].fillna(m_df[m_df.columns[df.columns.get_loc(main_c['Columns'][5])]])

    # Fix columns pt 1
    df[new_c['Columns'][4]] = df[new_c['Columns'][4]].fillna('LA')

    # Find conditions pt 1
    df[new_c['Columns'][4]] = np.where(
                    df[new_c['Columns'][10]] == -55,
                    df[new_c['Columns'][4]] + '|' + 'PD',
                    df[new_c['Columns'][4]]
    )

    # Find condition pt 2
    df['new'] = df[new_c['Columns'][11]]
    df[new_c['Columns'][11]] = get_string(df[new_c['Columns'][11]])

    df[new_c['Columns'][4]] = np.where(
                    df[new_c['Columns'][11]].isin(t_list['Name']),
                    df[new_c['Columns'][4]] + '|' + 'MI',
                    df[new_c['Columns'][4]]
    )
    df[new_c['Columns'][11]] = df['new']
    df.drop('new', axis=1, inplace=True)
    
    df[new_c['Columns'][4]] = df[new_c['Columns'][4]].str.split('|')

    # Expload column
    df = df.explode(
        new_c["Columns"][4]
    ).reset_index(drop=True)

    
    

    # Set proper stats
    df[new_c["Columns"][10]] = np.where(
                        df[new_c['Columns'][4]].isin(['LA','MI']),
                        None,
                        df[new_c["Columns"][10]]
    )

    df[new_c["Columns"][11]] = np.where(
                                df[new_c['Columns'][4]].isin(['LA','PD']),
                                None,
                                df[new_c["Columns"][11]]
    )

    # Set stats pt 2
    try:
        df[new_c["Columns"][7]] = m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][9])]].apply(time_to_float)
        df[new_c["Columns"][8]] = m_df[m_df.columns[m_df.columns.get_loc(main_c['Columns'][10])]].apply(time_to_float)
    except:
        None

    df[new_c["Columns"][7]] =df[new_c["Columns"][7]].fillna(6.0)
    df[new_c["Columns"][8]] = df[new_c["Columns"][8]].fillna(df[new_c["Columns"][7]] + df[new_c["Columns"][9]])

    df[new_c["Columns"][7]] = df[new_c["Columns"][7]].apply(float_to_time)
    df[new_c["Columns"][8]] = df[new_c["Columns"][8]].apply(float_to_time)

    df[new_c["Columns"][3]] = np.where(
                        df[new_c["Columns"][3]] == 'TRAINING',
                        "TRAIN",
                        df[new_c["Columns"][3]]
    )

    # fix columns pt 2
    df[new_c["Columns"][9]] = np.where(
        df[new_c["Columns"][4]].isin(['PD','MI']),
        None,
        df[new_c["Columns"][9]]
    )

    # fix columns pt 3
    df[new_c["Columns"][7]] = np.where(
        df[new_c["Columns"][4]].isin(['PD','MI']),
        None,
        df[new_c["Columns"][7]]
    )

    # fix columns pt 4
    df[new_c["Columns"][8]] = np.where(
        df[new_c["Columns"][4]].isin(['PD','MI']),
        None,
        df[new_c["Columns"][8]]
    )

    return df[df[new_c['Columns'][0]].notna() & (df[new_c['Columns'][0]] != '')]
