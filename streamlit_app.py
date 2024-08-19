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

            st.subheader("Main Data")
            st.dataframe(df)

            new_df = data_conversion(df,
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=6),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=7),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=0),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=1),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=2),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=4),
                        pd.read_excel(open(uploaded_tables, 'rb'), sheet_name=5))

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

            
            
            
