import streamlit as st
import pandas as pd
st.write ("""
# My first app
Hello *world!*
""")

sheet_id = "1wecLQmlElnGaUP92uVEgT0bdyqqwt4HTVlTaqyFFCIw"
sheet_name = "Question"
url_1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

sheet_name = "Game"
url_2 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

sheet_name = "Team"
url_3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

df_question = pd.read_csv(url_1)
df_game = pd.read_csv(url_2)
df_team = pd.read_csv(url_3)


print(df_question.head())

