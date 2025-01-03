import streamlit as st
import pandas as pd
import pytz
from scripts.main_script import data_conversion
from datetime import datetime, time
st.set_page_config(layout="wide")
# headers = st.secrets["APPROVAL_HEAD"]

@st.cache_data
def convert_df(df):
    """
    IMPORTANT: Cache the conversion to prevent computation on every rerun
    """
    return df.to_csv(index=False).encode('utf-8')

def app():
    st.title("CSV Converter")

    st.header("Introduction")
    st.write("Here we need to upload 2 files, an xlsx file and a csv file")

    st.header("Upload Tables")
    uploaded_tables = st.file_uploader("Choose an Excel file (The Tables)", type=["xlsx"])

    if uploaded_tables is not None:
        st.session_state.uploaded_tables = uploaded_tables
        
        st.header("Upload CSV")
        uploaded_file = st.file_uploader("Choose an Excel File (Convertable CSV)", type=['csv'])

        if uploaded_file is not None:
            st.session_state.uploaded_file = uploaded_file

            df = pd.read_csv(uploaded_file, encoding_errors='ignore')
            m_columns = pd.read_excel(uploaded_tables, sheet_name=6)
            n_columns = pd.read_excel(uploaded_tables, sheet_name=7)
            codes = pd.read_excel(uploaded_tables, sheet_name=0)
            subjobs = pd.read_excel(uploaded_tables, sheet_name=1)
            dept = pd.read_excel(uploaded_tables, sheet_name=2)
            admin_list = pd.read_excel(uploaded_tables, sheet_name=4)
            t_list = pd.read_excel(uploaded_tables, sheet_name=5)


            st.subheader("Main Data")
            st.dataframe(df)

            new_df, approval_df = data_conversion(df,m_columns, n_columns, codes, subjobs, dept, admin_list, t_list)
            # approval_df = approval_df[headers]
            # approval_df['Current Date'] = datetime.now()
            # approval_df['Current Date'] = pd.to_datetime(approval_df['Current Date'], utc=True)
            # central_time_zone = pytz.timezone('US/Central')
            # approval_df['Current Date'] = approval_df['Current Date'].dt.tz_convert(central_time_zone)
            
            # current_time_central = datetime.now(pytz.utc).astimezone(central_time_zone)
            # file_name = f"{current_time_central.strftime('%Y-%m-%d_%H-%M-%S')}_wait.csv"
            # csv_data = approval_df.to_csv(index=False)

            # st.subheader("Still Waiting for Approval")
            # st.dataframe(approval_df)
            # st.download_button(
            #     label='Waiting for Approval',
            #     data=csv_data,
            #     file_name=file_name,
            #     mime='text/csv'
            # )

            st.subheader("Converted Data")
            st.dataframe(new_df)

            st.subheader("Export Data")
            csv = convert_df(new_df)

            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name="converted.csv",
                mime='text/csv',
            )

            st.cache_data.clear

        else:

            st.write("Upload File!!!!!")

    else:
        st.write("Upload Tables")


if __name__ == "__main__":
    app()

            
            
            
