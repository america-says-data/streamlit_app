import streamlit as st
import pandas as pd
import pandasql as ps
from google.oauth2 import service_account
import gspread
import numpy as np
import matplotlib.pyplot as plt

st.write ("""
# Analysis Application of *America Says Data* - GSN Game Show
""")

 

gc = gspread.service_account(filename='.config/gspread/service_account.json')

#### TODO : figure out the versioning of oauth2 etc to handle secrets
#credentials = service_account.Credentials.from_service_account_info(
#    st.secrets["gcp_service_account"]
#)
#gc = gspread.Client(auth=credentials)

sheet = gc.open_by_key("1wecLQmlElnGaUP92uVEgT0bdyqqwt4HTVlTaqyFFCIw")

@st.cache_data(ttl=36000)
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

st.write("Last update - February 26th, 2023")

tab1, tab2= st.tabs(["Quick Question", "Stats"])

	
#### TODO : Create streamlit loading text that says "Creating Player Table"
@st.cache_data(ttl=86400)
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

	
	df_question[["Team_Member_Answer_1", "Team_Member_Answer_2"
		    , "Team_Member_Answer_3", "Team_Member_Answer_4"
		    , "Team_Member_Answer_5", "Team_Member_Answer_6"
		    , "Team_Member_Answer_7"]] =  df_question[["Team_Member_Answer_1", "Team_Member_Answer_2"
		    					, "Team_Member_Answer_3", "Team_Member_Answer_4"
		    					, "Team_Member_Answer_5", "Team_Member_Answer_6"
		    					, "Team_Member_Answer_7"]].apply(pd.to_numeric)
	
	
	df_question_tally = pd.melt(df_question,
                        id_vars =["Season", "Game", "Round", "Team", "Question", "Question_id"],
                        value_vars =["Team_Member_Answer_1", "Team_Member_Answer_2", "Team_Member_Answer_3", "Team_Member_Answer_4", "Team_Member_Answer_5", "Team_Member_Answer_6", "Team_Member_Answer_7"])

	df_question_tally_new = ps.sqldf("""
			select SEASON, GAME, TEAM, ROUND, VALUE, COUNT(*) as NUM_ANSWERS 
			from df_question_tally 
			group by SEASON, GAME, TEAM, ROUND, VALUE
			""")

	df_game[["Team_Member_Tiebreaker", "Team_Member_Bonus_A_1_1"
			, "Team_Member_Bonus_A_2_1", "Team_Member_Bonus_A_2_2"
			, "Team_Member_Bonus_A_3_1", "Team_Member_Bonus_A_3_2"
			, "Team_Member_Bonus_A_3_3","Team_Member_Bonus_A_4_1"
			, "Team_Member_Bonus_A_4_2", "Team_Member_Bonus_A_4_3"
			, "Team_Member_Bonus_A_4_4"]] = df_game[["Team_Member_Tiebreaker", "Team_Member_Bonus_A_1_1"
							 , "Team_Member_Bonus_A_2_1", "Team_Member_Bonus_A_2_2"
							 , "Team_Member_Bonus_A_3_1", "Team_Member_Bonus_A_3_2"
							 , "Team_Member_Bonus_A_3_3","Team_Member_Bonus_A_4_1"
							 , "Team_Member_Bonus_A_4_2", "Team_Member_Bonus_A_4_3"
							 , "Team_Member_Bonus_A_4_4"]].apply(pd.to_numeric)
	
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
print(df_players.head())

@st.cache_data(ttl=86400)
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

@st.cache_data(ttl=86400)
def best_bonus_round():
	return ps.sqldf("""
                select SEASON, GAME, WINNER, AFTER_SKIPPED_TIME_REMAINING, BONUS_Q_1, BONUS_Q_2, BONUS_Q_3, BONUS_Q_4
                from df_game
                where AFTER_SKIPPED_TIME_REMAINING is not null
                order by AFTER_SKIPPED_TIME_REMAINING desc
                """)

@st.cache_data(ttl=86400)
def worst_question():
	return ps.sqldf("""
                select distinct q.SEASON, q.GAME, q.ROUND, t.TEAM, ANSWERS_CORRECT_BY_ANSWERING_TEAM, QUESTION_TEXT
                from df_question q
                join df_team t
                        on q.TEAM = t.TEAM_NUM
                        and q.SEASON = t.SEASON
                        and q.GAME = t.GAME
                where ANSWERS_CORRECT_BY_ANSWERING_TEAM <=2 and QUESTION_TEXT <> 'NA'
                order by ANSWERS_CORRECT_BY_ANSWERING_TEAM
                """)

@st.cache_data(ttl=86400)
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

@st.cache_data(ttl=86400)
def top_player_overall():
	return ps.sqldf("""
                        select p.PLAYER, p.ANSWERS_CORRECT_NO_BONUS, p.TOTAL_ANSWERS_CORRECT
                        from df_players p
                        order by TOTAL_ANSWERS_CORRECT desc
			limit 11
                """)

@st.cache_data(ttl=86400)
def best_individual_round():
	return ps.sqldf("""
			select p.PLAYER, q.SEASON, q.GAME, q.QUESTION_TEXT, q.TIME_REMAINING
                        from (
			select PLAYER, SEASON, GAME, TEAM 
			, case when "1" = 7 then 1
				when "2" = 7 then 2
				else 3 end as ROUND
                        from df_players
			where "1" = 7 or "2" = 7 or "3" = 7
			) p
			join df_question q
				on p.SEASON = q.SEASON
				and p.GAME = q.GAME
				and p.TEAM = q.TEAM
				and p.ROUND = q.ROUND
			order by q.TIME_REMAINING desc
                """)



def option_case(answer_string):
	new_string_list = []
	for i, l in enumerate(answer_string):
		if i == 0 or answer_string[i-1] == ' ' or answer_string[i] == ' ' or answer_string[i] == '-':
			new_string_list.append(l)
		else:
			new_string_list.append('_')
	return ''.join(new_string_list).upper()
		

def question_callback():
	st.session_state.question_button = True

def answer_callback_1():
	st.session_state.answer_button_1 = True
def answer_callback_2():
	st.session_state.answer_button_2 = True
def answer_callback_3():
	st.session_state.answer_button_3 = True
def answer_callback_4():
	st.session_state.answer_button_4 = True
def answer_callback_5():
	st.session_state.answer_button_5 = True
def answer_callback_6():
	st.session_state.answer_button_6 = True
def answer_callback_7():
	st.session_state.answer_button_7 = True

def answer_reset():
	st.session_state.answer_button_1 = False
	st.session_state.answer_button_2 = False
	st.session_state.answer_button_3 = False
	st.session_state.answer_button_4 = False
	st.session_state.answer_button_5 = False
	st.session_state.answer_button_6 = False
	st.session_state.answer_button_7 = False
	
### PAGE LAYOUT

with tab1:
	st.header("Random America Says Question")
	
	if st.button('Produce Question!'):
		if 'question' in st.session_state:
			del st.session_state.question
		if 'question' not in st.session_state:
			st.session_state.question = df_question[df_question.Question_Text.notnull()].sample()
			if 'question_button' not in st.session_state:
				st.session_state.question_button = False
			if 'answer_button_1' not in st.session_state:
				st.session_state.answer_button_1 = False
			else:
				st.session_state.answer_button_1 = False
			if 'answer_button_2' not in st.session_state:
				st.session_state.answer_button_2 = False
			else:
				st.session_state.answer_button_2 = False
			if 'answer_button_3' not in st.session_state:
				st.session_state.answer_button_3 = False
			else:
				st.session_state.answer_button_3 = False
			if 'answer_button_4' not in st.session_state:
				st.session_state.answer_button_4 = False
			else:
				st.session_state.answer_button_4 = False
			if 'answer_button_5' not in st.session_state:
				st.session_state.answer_button_5 = False
			else:
				st.session_state.answer_button_5 = False
			if 'answer_button_6' not in st.session_state:
				st.session_state.answer_button_6 = False
			else:
				st.session_state.answer_button_6 = False
			if 'answer_button_7' not in st.session_state:
				st.session_state.answer_button_7 = False
			else:
				st.session_state.answer_button_7 = False
			
			
			
	if 'question' in st.session_state:
		if ( st.button('Reveal Question', on_click = question_callback) 
			      or st.session_state.question_button):
			st.subheader(st.session_state.question.iloc[0]['Question_Text'])
		
			answer_1 = st.session_state.question.iloc[0]['Answer_1']
			answer_2 = st.session_state.question.iloc[0]['Answer_2']
			answer_3 = st.session_state.question.iloc[0]['Answer_3']
			answer_4 = st.session_state.question.iloc[0]['Answer_4']
			answer_5 = st.session_state.question.iloc[0]['Answer_5']
			answer_6 = st.session_state.question.iloc[0]['Answer_6']
			answer_7 = st.session_state.question.iloc[0]['Answer_7']
			
			
			if (st.button(option_case(answer_1), on_click = answer_callback_1, key = 1) or st.session_state.answer_button_1):
				st.write(answer_1)
			if (st.button(option_case(answer_2), on_click = answer_callback_2, key = 2) or st.session_state.answer_button_2):
				st.write(answer_2)
			if (st.button(option_case(answer_3), on_click = answer_callback_3, key = 3) or st.session_state.answer_button_3):
				st.write(answer_3)
			if (st.button(option_case(answer_4), on_click = answer_callback_4, key = 4) or st.session_state.answer_button_4):
				st.write(answer_4)
			if (st.button(option_case(answer_5), on_click = answer_callback_5, key = 5) or st.session_state.answer_button_5):
				st.write(answer_5)
			if (st.button(option_case(answer_6), on_click = answer_callback_6, key = 6) or st.session_state.answer_button_6):
				st.write(answer_6)
			if (st.button(option_case(answer_7), on_click = answer_callback_7, key = 7) or st.session_state.answer_button_7):
				st.write(answer_7)

				
		st.button('Reset Answers', on_click = answer_reset)
	


with tab2:
	season_select = st.selectbox(
    		'What season would you like to look at?',
		    ('All Seasons', '1', '2', '3', '4', '5'))

	if season_select == "All Seasons":
		season_select_clause = "IN ('1','2','3','4','5')"
	elif season_select == "1":
		season_select_clause = "= '1'"
	elif season_select == "2":
		season_select_clause = "= '2'"
	elif season_select == "3":
		season_select_clause = "= '3'"
	elif season_select == "4":
		season_select_clause = "= '4'"
	elif season_select == "5":
		season_select_clause = "= '5'"
	

	st.write("Histogram of Answers Correct (by answering team)")

	df_dist = ps.sqldf("""select ANSWERS_CORRECT_BY_ANSWERING_TEAM, 
		100.00*COUNT(*) / (select count(*) 
				from df_question 
				where QUESTION_TEXT <> 'NA' and QUESTION_TEXT is not null AND QUESTION_TEXT <> ''
				and SEASON {season_select_clause}
				) as 'Percent Times that Number of Answers is Provided'
                from df_question
		where QUESTION_TEXT <> 'NA' and QUESTION_TEXT is not null AND QUESTION_TEXT <> ''
		and SEASON {season_select_clause}
		group by ANSWERS_CORRECT_BY_ANSWERING_TEAM
                order by ANSWERS_CORRECT_BY_ANSWERING_TEAM 
                """.format(season_select_clause=season_select_clause))
	df_dist.reset_index(inplace=True)
	df_dist = df_dist.set_index("Answers_Correct_By_Answering_Team")

	st.bar_chart(df_dist[["Percent Times that Number of Answers is Provided"]])


	st.write("Histogram of Answers Correct by Round (by answering team)")


	df_dist_round = ps.sqldf("""select q.ROUND, ANSWERS_CORRECT_BY_ANSWERING_TEAM, 
		100.00*COUNT(*) / rc.rc_num as 'Percent Times that Number of Answers is Provided'
                from df_question q
			join (select ROUND, count(*) rc_num 
					from df_question 
					where QUESTION_TEXT <> 'NA' and QUESTION_TEXT is not null and QUESTION_TEXT <> ''
					and SEASON {season_select_clause}
					group by ROUND
					) rc
			on q.ROUND = rc.ROUND
		where QUESTION_TEXT <> 'NA' and QUESTION_TEXT is not null AND QUESTION_TEXT <> ''
		and SEASON {season_select_clause}
		group by q.ROUND, ANSWERS_CORRECT_BY_ANSWERING_TEAM
                order by ANSWERS_CORRECT_BY_ANSWERING_TEAM 
                """.format(season_select_clause=season_select_clause))

	df_dist_round = df_dist_round[["Answers_Correct_By_Answering_Team", "Round", "Percent Times that Number of Answers is Provided"]].pivot(
										index = "Answers_Correct_By_Answering_Team"
										, columns="Round"
									     	, values="Percent Times that Number of Answers is Provided"
										)


	df_dist_round_st = df_dist_round
	df_dist_round_st.reset_index(inplace=True)
	df_dist_round_st = df_dist_round_st.set_index("Answers_Correct_By_Answering_Team")


	df_dist_round_st = df_dist_round_st.rename_axis(None).rename(columns = {1: "Round 1", 2:"Round 2", 3:"Round 3"})

	st.line_chart(df_dist_round_st)

#### TODO: update the visuals (titles, axis, etc)
#fig = df_dist_round.plot(kind="bar").figure
#st.pyplot(fig)


	st.write("Average answers cleaned up by Season")
	df_season_cleanup = ps.sqldf("""select SEASON as 'Season', avg(ANSWERS_CORRECT_BY_CLEAN_UP_TEAM) as 'Average Answers Cleaned Up'
					from (
					select SEASON, ANSWERS_CORRECT_BY_CLEAN_UP_TEAM
					from df_question
					where TEAM_MEMBER_ANSWER_1 <> -1
					and TEAM_MEMBER_ANSWER_2 <> -1
					and TEAM_MEMBER_ANSWER_3 <> -1
					and TEAM_MEMBER_ANSWER_4 <> -1
					and TEAM_MEMBER_ANSWER_5 <> -1
					and TEAM_MEMBER_ANSWER_6 <> -1
					and TEAM_MEMBER_ANSWER_7 <> -1
					and ANSWERS_CORRECT_BY_CLEAN_UP_TEAM <> 'NA'
					and ANSWERS_CORRECT_BY_CLEAN_UP_TEAM is not null
					)
					group by SEASON
					order by SEASON
					""")
	df_season_cleanup.reset_index(inplace=True)
	df_season_cleanup = df_season_cleanup.set_index("Season")
	
	st.bar_chart(df_season_cleanup[["Average Answers Cleaned Up"]])


	option = st.selectbox(
    		'What would you like to explore?',
		    ('Best Question', 'Worst Question', 'Best Bonus Round', 'Top Player of Team', 'Top Player Overall', 'Best Individual Round', 'Custom Query...'))

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
	elif option == 'Best Individual Round':
		final_df = best_individual_round()

	else:
		text_input = st.text_input(
	        "Write your query here (df_question, df_game, df_team, df_players, df_round)",
        	label_visibility="visible",
	        disabled=False,
        	placeholder="QUERY",
    		)
		final_df = ps.sqldf(text_input)
	

	st.dataframe(final_df)


