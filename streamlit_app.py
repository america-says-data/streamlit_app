import streamlit as st
import pandas as pd
import pandasql as ps
import gspread
import numpy as np

st.write ("""
# Analysis Application of *America Says Data* - GSN Game Show
""")

 

gc = gspread.service_account(filename='.config/gspread/service_account.json')


sheet =gc.open_by_key("1wecLQmlElnGaUP92uVEgT0bdyqqwt4HTVlTaqyFFCIw")

@st.cache_data
def get_tables():
	df_question = pd.DataFrame(sheet.worksheet("Question").get_all_records())
	df_game = pd.DataFrame(sheet.worksheet("Game").get_all_records())
	df_team = pd.DataFrame(sheet.worksheet("Team").get_all_records())
	df_round = pd.DataFrame(sheet.worksheet("Round").get_all_records())
	df_question = df_question.replace('NA', np.nan)
	df_game = df_game.replace('NA', np.nan)
	df_team = df_team.replace('NA', np.nan)
		
	df_question["Time_Remaining"] = df_question["Time_Remaining"].astype(float)
	df_game["After_Skipped_Time_Remaining"] = df_game["After_Skipped_Time_Remaining"].astype(float)

	return df_question, df_game, df_team, df_round

df_question, df_game, df_team, df_round = get_tables()

st.write("Currently built off of ", len(df_game), " games")

st.write("Last update - February 8th, 2023")


#### TODO : Create streamlit loading text that says "Creating Player Table"
#@st.cache_data
def build_players_table():
	df_individual = df_team[["Season", "Game", "Game_id", "Team", "Team_Num", "Team_id", "Captain", "Member_2", "Member_3", "Member_4"]]
	df_individual = pd.melt(df_individual, 
			id_vars =["Season", "Game", "Game_id", "Team", "Team_Num", "Team_id"], 
			value_vars =["Captain", "Member_2", "Member_3", "Member_4"])


	df_individual["Player_Number"] = np.where(df_individual['variable']=="Captain", 1,
                   np.where(df_individual['variable']=="Member_2", 2,
                   np.where(df_individual['variable']=="Member_3", 3, 4)))


	df_individual["Player"] = df_individual["value"]+"-"+df_individual["Team"]
	df_individual["Captain"] = np.where(df_individual['variable']=="Captain", True, False)


	df_question_tally = pd.melt(df_question,
                        id_vars =["Season", "Game", "Round", "Team", "Question", "Question_id"],
                        value_vars =["Team_Member_Answer_1", "Team_Member_Answer_2", "Team_Member_Answer_3", "Team_Member_Answer_4", "Team_Member_Answer_5", "Team_Member_Answer_6", "Team_Member_Answer_7"])

	df_question_tally_new = ps.sqldf("""
			select SEASON, GAME, TEAM, ROUND, VALUE, COUNT(*) as NUM_ANSWERS
			from df_question_tally
			group by SEASON, GAME, TEAM, ROUND, VALUE
			""")


	df_bonus_tally = pd.melt(df_game,
                        id_vars =["Season", "Game", "Game_id", "Team_1", "Winner"],
                        value_vars =["Team_Member_Tiebreaker", "Team_Member_Bonus_A_1_1", "Team_Member_Bonus_A_2_1", "Team_Member_Bonus_A_2_2", "Team_Member_Bonus_A_3_1", "Team_Member_Bonus_A_3_2", "Team_Member_Bonus_A_3_3","Team_Member_Bonus_A_4_1", "Team_Member_Bonus_A_4_2", "Team_Member_Bonus_A_4_3", "Team_Member_Bonus_A_4_4"])

	df_bonus_tally_new = ps.sqldf("""
                        select SEASON, GAME, CASE WHEN TEAM_1 = WINNER THEN 1 ELSE 2 END as "Team", "B" as "Round", VALUE, COUNT(*) as NUM_ANSWERS
                        from df_bonus_tally
                        group by SEASON, GAME, TEAM, ROUND, VALUE
                        """)


	df_tally = df_question_tally_new.append(df_bonus_tally_new)


	player_join_df = ps.sqldf("""
        select i.SEASON, i.GAME, q.ROUND, i.TEAM, i.PLAYER, i.PLAYER_NUMBER, q.NUM_ANSWERS 
        from df_individual i
        left join df_tally q
		on i.SEASON = q.SEASON
		and i.GAME = q.GAME
		and i.TEAM_NUM = q.TEAM
		and i.PLAYER_NUMBER = CAST(q.VALUE AS INT)
        """)



	df_player_unmelt = player_join_df.pivot(index = ["Season", "Game", "Team", "Player", "Player_Number"], columns = "Round", values = "NUM_ANSWERS").reset_index()



	df_player_unmelt = df_player_unmelt.fillna(0)

	df_player_unmelt["Answers_Correct_No_Bonus"] = df_player_unmelt["1"]+df_player_unmelt["2"]+df_player_unmelt["3"]
	df_player_unmelt["Total_Answers_Correct"] = df_player_unmelt["1"]+df_player_unmelt["2"]+df_player_unmelt["3"]+df_player_unmelt["B"]

	return df_player_unmelt

df_players = build_players_table()

@st.cache_data
def best_question():
	return ps.sqldf("""
        select distinct q.SEASON, q.GAME, q.ROUND, t.TEAM, TIME_REMAINING, QUESTION_TEXT
        from df_question q
        join df_team t
                on q.TEAM = t.TEAM_NUM
                and q.SEASON = t.SEASON
                and q.GAME = t.GAME
        where TIME_REMAINING is not null
        order by TIME_REMAINING desc
        """)

@st.cache_data
def best_bonus_round():
	return ps.sqldf("""
                select SEASON, GAME, WINNER, AFTER_SKIPPED_TIME_REMAINING, BONUS_Q_1, BONUS_Q_2, BONUS_Q_3, BONUS_Q_4
                from df_game
                where AFTER_SKIPPED_TIME_REMAINING is not null
                order by AFTER_SKIPPED_TIME_REMAINING desc
                """)

@st.cache_data
def worst_question():
	return ps.sqldf("""
                select distinct q.SEASON, q.GAME, q.ROUND, t.TEAM, ANSWERS_CORRECT_BY_ANSWERING_TEAM, QUESTION_TEXT
                from df_question q
                join df_team t
                        on q.TEAM = t.TEAM_NUM
                        and q.SEASON = t.SEASON
                        and q.GAME = t.GAME
                where ANSWERS_CORRECT_BY_ANSWERING_TEAM <=2
                order by ANSWERS_CORRECT_BY_ANSWERING_TEAM
                """)

@st.cache_data
def top_player_of_team():
	return ps.sqldf("""
                select p.PLAYER, p.TEAM, p.TOTAL_ANSWERS_CORRECT, t.TOTAL_ANSWERS
			, 100*p.TOTAL_ANSWERS_CORRECT / t.TOTAL_ANSWERS as PERCENT_OF_TEAM_ANSWERS
                from df_players p
                join df_team t
                        on p.SEASON = t.SEASON
                        and p.GAME = t.GAME
                        and p.TEAM = t.TEAM
                order by PERCENT_OF_TEAM_ANSWERS desc
		limit 11
                """)

@st.cache_data
def top_player_overall():
	return ps.sqldf("""
                        select p.PLAYER, p.ANSWERS_CORRECT_NO_BONUS, p.TOTAL_ANSWERS_CORRECT
                        from df_players p
                        order by TOTAL_ANSWERS_CORRECT desc
			limit 11
                """)



option = st.selectbox(
    'What would you like to explore?',
    ('Best Question', 'Worst Question', 'Best Bonus Round', 'Top Player of Team', 'Top Player Overall', 'Custom Query...'))

if option == 'Best Question':
	final_df = best_question()
elif option == 'Best Bonus Round':
	final_df = best_bonus_round()
elif option == 'Worst Question':
	final_df = worst_question()
elif option == 'Top Player Overall':
        final_df = top_player_overall()
elif option == 'Top Player of Team':
        final_df = top_player_of_team()

else:
	text_input = st.text_input(
        "Write your query here (df_question, df_game, df_team, df_players, df_round)",
        label_visibility="visible",
        disabled=False,
        placeholder="QUERY",
    )
	final_df = ps.sqldf(text_input)
	

st.dataframe(final_df)


