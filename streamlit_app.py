import streamlit as st
import pandas as pd
from scripts.main_script import data_conversion


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

            df = pd.read_csv(uploaded_file)
            m_columns = pd.read_excel(uploaded_tables, sheet_name=6)
            n_columns = pd.read_excel(uploaded_tables, sheet_name=7)
            codes = pd.read_excel(uploaded_tables, sheet_name=0)
            subjobs = pd.read_excel(uploaded_tables, sheet_name=1)
            dept = pd.read_excel(uploaded_tables, sheet_name=2)
            admin_list = pd.read_excel(uploaded_tables, sheet_name=4)
            t_list = pd.read_excel(uploaded_tables, sheet_name=5)

            st.subheader("Main Data")
            st.dataframe(df)

            new_df = data_conversion(df,m_columns, n_columns, codes, subjobs, dept, admin_list, t_list)

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

            
            
            
