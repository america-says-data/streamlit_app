import streamlit as st
import pandas as pd
import pandasql as ps
import gspread
import numpy as np

st.write ("""
# QUICK APP - Analysis of *America Says Data* - GSN Game Show
""")

 

gc = gspread.service_account()


sheet =gc.open_by_key("1wecLQmlElnGaUP92uVEgT0bdyqqwt4HTVlTaqyFFCIw")

df_question = pd.DataFrame(sheet.worksheet("Question").get_all_records())
df_game = pd.DataFrame(sheet.worksheet("Game").get_all_records())
df_team = pd.DataFrame(sheet.worksheet("Team").get_all_records())

st.write("Currently built off of ", len(df_game), " games")

st.write("Last update - February 7th, 2023")

df_question = df_question.replace('NA', np.nan)
df_game = df_game.replace('NA', np.nan)
df_team = df_team.replace('NA', np.nan)

df_question["Time_Remaining"] = df_question["Time_Remaining"].astype(float)
df_game["After_Skipped_Time_Remaining"] = df_game["After_Skipped_Time_Remaining"].astype(float)



option = st.selectbox(
    'What would you like to explore?',
    ('Best Round', 'Worst Round', 'Best Bonus Round', 'Custom Query...'))

if option == 'Best Round':
	final_df = ps.sqldf("""
	select distinct q.SEASON, q.GAME, q.ROUND, t.TEAM, TIME_REMAINING, QUESTION_TEXT
	from df_question q 
	join df_team t 
		on q.TEAM = t.TEAM_NUM
		and q.SEASON = t.SEASON
		and q.GAME = t.GAME
	where TIME_REMAINING is not null
	order by TIME_REMAINING desc
	""")

elif option == 'Best Bonus Round':
	final_df = ps.sqldf("""
		select SEASON, GAME, WINNER, AFTER_SKIPPED_TIME_REMAINING, BONUS_Q_1, BONUS_Q_2, BONUS_Q_3, BONUS_Q_4
		from df_game
		where AFTER_SKIPPED_TIME_REMAINING is not null
		order by AFTER_SKIPPED_TIME_REMAINING desc
		""")

elif option == 'Worst Round':
	final_df = ps.sqldf("""
                select distinct q.SEASON, q.GAME, q.ROUND, t.TEAM, ANSWERS_CORRECT_BY_ANSWERING_TEAM, QUESTION_TEXT
        	from df_question q
        	join df_team t
                	on q.TEAM = t.TEAM_NUM
                	and q.SEASON = t.SEASON
                	and q.GAME = t.GAME
                where ANSWERS_CORRECT_BY_ANSWERING_TEAM <=2
                order by ANSWERS_CORRECT_BY_ANSWERING_TEAM
                """)

else:
	text_input = st.text_input(
        "Write your query here (df_question, df_game, df_team)",
        label_visibility="visible",
        disabled=False,
        placeholder="QUERY",
    )
	final_df = ps.sqldf(text_input)
	

st.dataframe(final_df)


