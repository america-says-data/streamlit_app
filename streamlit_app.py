import streamlit as st
import pandas as pd
import pandasql as ps
from google.oauth2 import service_account
import gspread
import numpy as np
import matplotlib.pyplot as plt
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objs as go
from sklearn.linear_model import LogisticRegression

st.write ("""
# Analysis Application of *America Says Data* - GSN Game Show
""")

 

gc = gspread.service_account(filename='.config/gspread/service_account.json')


#### TODO : figure out the versioning of oauth2 etc to handle secrets
# scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["gcp_service_account"], scopes = scopes
# )
# # gc = gspread.oauth(scopes=scopes)
# gc = gspread.oauth_from_dict(credentials=credentials)

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

	df_round = df_round[df_round["Season"] != ""]		
	df_question["Time_Remaining"] = df_question["Time_Remaining"].astype(float)
	df_game["After_Skipped_Time_Remaining"] = df_game["After_Skipped_Time_Remaining"].astype(float)

	df_question["Use_Question_Clean_Up"] = np.where((df_question["Team_Member_Answer_1"] == -1) |
							(df_question["Team_Member_Answer_2"] == -1) |
							(df_question["Team_Member_Answer_3"] == -1) |
							(df_question["Team_Member_Answer_4"] == -1) |
							(df_question["Team_Member_Answer_5"] == -1) |
							(df_question["Team_Member_Answer_6"] == -1) |
							(df_question["Team_Member_Answer_7"] == -1) | 
							(df_question["Answers_Correct_By_Clean_Up_Team"].isna()), 0, 1)
							
	
	return df_question, df_game, df_team, df_round

df_question, df_game, df_team, df_round = get_tables()

st.write("Currently built off of ", len(df_game), " games")

st.write("Last update - March 15th, 2023")

tab1, tab2, tab3 = st.tabs(["Quick Question", "Stats", "Game Select"])

	
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


@st.cache_data(ttl=86400)
def build_hattots():
	df_round["Score_total"] = df_round["Score_total"].astype(int)
	df_round["Score_after_round"] = df_round[["Team_id", "Score_total"]].groupby("Team_id").cumsum()
	df_round["Score_before_round"] = df_round[["Team_id", "Score_after_round"]].groupby("Team_id").shift(1)
	df_round["Opponent"] = np.where(df_round.Team_id.str[-1:] == '1', df_round.Team_id.str[:-1]+'2',df_round.Team_id.str[:-1]+'1')
	df_round["Opponent_score_after_round"] = df_round[["Opponent", "Round"]].merge(df_round[["Team_id", "Round", "Score_after_round"]], left_on = ["Opponent", "Round"], right_on = ["Team_id", "Round"])[["Score_after_round"]]
	df_round["Opponent_score_before_round"] = df_round[["Opponent", "Round"]].merge(df_round[["Team_id", "Round", "Score_before_round"]], left_on = ["Opponent", "Round"], right_on = ["Team_id", "Round"])[["Score_before_round"]]
	df_round["Question_Text"] =  df_round[["Question_id"]].merge(df_question[["Question_id", "Question_Text"]], on = "Question_id")[["Question_Text"]]

	df_round_1 = df_round[df_round.Question_Text.notnull()]
	df_round_2 = df_round[(df_round.Question_Text.notnull()) & (df_round.Round == 3)]
	df_round_3 = df_round[(df_round.Question_Text.notnull()) & (df_round.Round == 3)
                                & (df_round.Opponent_score_before_round >= df_round.Score_before_round)
                                & (df_round.Opponent_score_after_round >
                                        df_round.Score_before_round + (df_round.Score_total - df_round.Score_no_clean_up) + 1800)]

	df_round_1_agg = ps.sqldf("""select SEASON
                                        , count(*) as 'Total_questions'
                                        , SUM(CASE WHEN SCORE_TOTAL > SCORE_TOTAL_NO_BONUS THEN 1 ELSE 0 END) as 'Boards_Cleared'
                                        from df_round_1
                                        group by SEASON""")

	df_round_2_agg = ps.sqldf("""select SEASON
                                        , count(*) as 'Third_Round_Total_questions'
                                        , SUM(CASE WHEN SCORE_TOTAL > SCORE_TOTAL_NO_BONUS THEN 1 ELSE 0 END) as 'Third_Round_Boards_Cleared'
                                        from df_round_2
                                        group by SEASON""")

	df_round_3_agg = ps.sqldf("""select SEASON
                                        , count(*) as 'Third_Round_Deficit_Total_questions'
                                        , SUM(CASE WHEN SCORE_TOTAL > SCORE_TOTAL_NO_BONUS THEN 1 ELSE 0 END) as 'Third_Round_Deficit_Boards_Cleared'
                                        from df_round_3
                                        group by SEASON""")

	df_hattots = ps.sqldf("""select o.SEASON
                                        , Total_questions
                                        , Boards_Cleared
                                        , Third_Round_Total_questions
                                        , Third_Round_Boards_Cleared
                                        , Third_Round_Deficit_Total_questions
                                        , Third_Round_Deficit_Boards_Cleared
                                        from df_round_1_agg o
                                        join df_round_2_agg t on o.SEASON = t.SEASON
                                        join df_round_3_agg e on t.SEASON = e.SEASON
                                        """)
	df_hattots =  ps.sqldf("""select * from df_hattots
                                UNION
                                select 'Total' as SEASON
                                        , sum(Total_questions) Total_questions
                                        , sum(Boards_Cleared) Boards_Cleared
                                        , sum(Third_Round_Total_questions) Third_Round_Total_questions
                                        , sum(Third_Round_Boards_Cleared) Third_Round_Boards_Cleared
                                        , sum(Third_Round_Deficit_Total_questions) Third_Round_Deficit_Total_questions
                                        , sum(Third_Round_Deficit_Boards_Cleared) Third_Round_Deficit_Boards_Cleared
                                from df_hattots
                                """)
	
	df_hattots = ps.sqldf("""select SEASON
					, Total_questions
					, Boards_Cleared
					, 100 * Boards_Cleared / Total_questions Percent_Bonus
 					, Third_Round_Total_questions
                                        , Third_Round_Boards_Cleared
					, 100 * Third_Round_Boards_Cleared / Third_Round_Total_questions Percent_Bonus_Third_Round
                                        , Third_Round_Deficit_Total_questions
                                        , Third_Round_Deficit_Boards_Cleared
					, 100 * Third_Round_Deficit_Boards_Cleared / Third_Round_Deficit_Total_questions Percent_Bonus_Winning_Deficit	
					from df_hattots
				""")
	df_hattots["Percent_Bonus"] = df_hattots["Percent_Bonus"].astype(str)+'%'
	df_hattots["Percent_Bonus_Third_Round"] = df_hattots["Percent_Bonus_Third_Round"].astype(str)+'%'
	df_hattots["Percent_Bonus_Winning_Deficit"] = df_hattots["Percent_Bonus_Winning_Deficit"].astype(str)+'%'
	return df_hattots

df_players = build_players_table()
df_hattots = build_hattots()

@st.cache_data(ttl=86400)
def create_probability():
	df_win_prediction = df_team[df_team.Bonus_Rounds_Complete.notnull()][['Score_check', 'Bonus_Rounds_Complete']]
	df_win_prediction['win'] = np.where(df_win_prediction['Bonus_Rounds_Complete'] == 4, 1, 0)
	
	# extract x y for prediction
	X = df_win_prediction.Score_check
	y =  df_win_prediction.win
	# logistic regression for prediction
	logreg = LogisticRegression(random_state=13).fit(X.values.reshape(-1,1), y)

	test_score = np.arange(0, 14400, 100)
	test_probabilities = logreg.predict_proba(test_score.reshape(-1,1))[:,1]
	df_win_probability = pd.DataFrame(zip(test_score,test_probabilities), columns = ['test_score','test_probabilities'])

	current_win_rate = sum(df_win_prediction.win) / len(df_win_prediction)
	return current_win_rate, df_win_probability

win_rate, win_prob = create_probability()

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
                where ANSWERS_CORRECT_BY_ANSWERING_TEAM <=1 and QUESTION_TEXT <> 'NA'
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
			select p.PLAYER, q.TIME_REMAINING, q.QUESTION_TEXT, q.ROUND, q.SEASON, q.GAME, q.DATE, q.YEAR
                        from (
			select PLAYER, SEASON, GAME, TEAM 
			, case when "1" = 7 then 1
				when "2" = 7 then 2
				else 3 end as ROUND
                        from df_players
			where "1" = 7 or "2" = 7 or "3" = 7
			) p
			join df_team t on t.TEAM = p.TEAM and t.GAME = p.GAME and t.SEASON = p.SEASON
			join df_question q
				on p.SEASON = q.SEASON
				and p.GAME = q.GAME
				and t.TEAM_NUM = q.TEAM
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
	
#################################################################################################################################################
#### FIRST TAB!!! RANDOM QUESTION
####
#################################################################################################################################################


with tab1:
	st.header("Random America Says Question")
	
	if st.button('Produce Question!', use_container_width = True):
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
	

#################################################################################################################################################
#### SECOND TAB!!! STATS
####
#################################################################################################################################################
with tab2:
##----------------------------------------------------------------------------------------------------------------------------------------------------
## Season collection
##----------------------------------------------------------------------------------------------------------------------------------------------------
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
	

##----------------------------------------------------------------------------------------------------------------------------------------------------
## Answers correct histogram (by season)
##----------------------------------------------------------------------------------------------------------------------------------------------------

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

##----------------------------------------------------------------------------------------------------------------------------------------------------
## Answers correct by round (by season)
##----------------------------------------------------------------------------------------------------------------------------------------------------

	
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
	
	st.markdown("""---""")
#### TODO: update the visuals (titles, axis, etc)
##----------------------------------------------------------------------------------------------------------------------------------------------------
## percent chance question breakdown
##----------------------------------------------------------------------------------------------------------------------------------------------------
	st.write("'It happens all the time on this show!'")
	st.write("""Does it? Here we break down how many times teams clear the board, clear the board in the 3rd round, 
				and clear the board in the third round when it's the only option for victory.""")
		
	st.dataframe(df_hattots.set_index("Season"))	

	st.markdown("""---""")

	
##----------------------------------------------------------------------------------------------------------------------------------------------------
## season clean up comparison (covid season 4 drop)
##----------------------------------------------------------------------------------------------------------------------------------------------------

	st.write("Average answers by Season")
	df_season_cleanup = ps.sqldf("""select SEASON as 'Season', avg(ANSWERS_CORRECT_BY_ANSWERING_TEAM) as 'Average Answers'
					from (
					select SEASON, ANSWERS_CORRECT_BY_ANSWERING_TEAM
					from df_question
					)
					group by SEASON
					order by SEASON
					""")
	df_season_cleanup.reset_index(inplace=True)
	df_season_cleanup = df_season_cleanup.set_index("Season")
	
	st.bar_chart(df_season_cleanup[["Average Answers"]])
	
##----------------------------------------------------------------------------------------------------------------------------------------------------
## season clean up comparison (covid season 4 drop)
##----------------------------------------------------------------------------------------------------------------------------------------------------

	st.write("Average answers cleaned up by Season")
	df_season_cleanup = ps.sqldf("""select SEASON as 'Season'
					, avg(ANSWERS_CORRECT_BY_CLEAN_UP_TEAM) as 'Average Answers Cleaned Up'
					, avg(ANSWERS_MISSED_BY_CLEAN_UP_TEAM) as 'Average Answers Missed by Both Teams'
					, sum(ANSWERS_CORRECT_BY_CLEAN_UP_TEAM) / sum(CLEAN_UP_OPPORTUNITIES) as 'Percent Possible Answers Cleaned Up'
					from (
					select SEASON, ANSWERS_CORRECT_BY_CLEAN_UP_TEAM,
					7 - (ANSWERS_CORRECT_BY_ANSWERING_TEAM + ANSWERS_CORRECT_BY_CLEAN_UP_TEAM) as ANSWERS_MISSED_BY_CLEAN_UP_TEAM,
					7 - ANSWERS_CORRECT_BY_ANSWERING_TEAM as CLEAN_UP_OPPORTUNITIES
					from df_question
					where USE_QUESTION_CLEAN_UP = 1
					)
					group by SEASON
					order by SEASON
					""")
	df_season_cleanup.reset_index(inplace=True)
	df_season_cleanup = df_season_cleanup.set_index("Season")
	
	st.line_chart(df_season_cleanup[["Average Answers Cleaned Up", "Average Answers Missed by Both Teams", "Percent Possible Answers Cleaned Up"]])

	st.markdown("""---""")
	
##----------------------------------------------------------------------------------------------------------------------------------------------------
## prediction chart
##----------------------------------------------------------------------------------------------------------------------------------------------------

	fig = px.line(win_prob, x="test_score", y="test_probabilities", labels={
                     "test_score": "Team Score Before Bonus Round",
                     "test_probabilities": "Probability of Winning $15,000 in the Bonus Round"
                 }, title='Probability of Win Based on Team Score')
	fig.add_hline(y=win_rate, line_dash="dot", annotation_text="Historical Win Rate: {:.2%}".format(win_rate), annotation_position="bottom right")
	
	st.plotly_chart(fig, use_container_width=True)


	
##----------------------------------------------------------------------------------------------------------------------------------------------------
## stat selection option
##----------------------------------------------------------------------------------------------------------------------------------------------------

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

##----------------------------------------------------------------------------------------------------------------------------------------------------
## question finder
##----------------------------------------------------------------------------------------------------------------------------------------------------
	
	question_input = st.text_input(
	        "What question are you looking for?")

	df_total = ps.sqldf("""SELECT QUESTION_TEXT, ANSWER_1, ANSWER_2, ANSWER_3, ANSWER_4, ANSWER_5, ANSWER_6, ANSWER_7 
					FROM df_question where QUESTION_TEXT like '%{}%'
					limit 10
					""".format(question_input))
	df_b1 = ps.sqldf("""SELECT BONUS_Q_1 as Question_Text, BONUS_A_1_1 as Answer_1, 'B1' AS Answer_2, 'B1' AS Answer_3, 'B1' AS Answer_4, 'B1' AS Answer_5, 'B1' AS Answer_6, 'B1' AS Answer_7 
					FROM df_game where BONUS_Q_1 like '%{}%'
					limit 10
					""".format(question_input))
	df_b2 = ps.sqldf("""SELECT BONUS_Q_2 as Question_Text, BONUS_A_2_1 as Answer_1, BONUS_A_2_2 as Answer_2, 'B2' as Answer_3, 'B2' AS Answer_4, 'B2' AS Answer_5, 'B2' AS Answer_6, 'B2' AS Answer_7 
					FROM df_game where BONUS_Q_2 like '%{}%'
					limit 10
					""".format(question_input))
	df_b3 = ps.sqldf("""SELECT BONUS_Q_3 as Question_Text, BONUS_A_3_1 as Answer_1, BONUS_A_3_2 as Answer_2, BONUS_A_3_3 as Answer_3, 'B3' AS Answer_4, 'B3' AS Answer_5, 'B3' AS Answer_6, 'B3' AS Answer_7 
					FROM df_game where BONUS_Q_3 like '%{}%'
					limit 10
					""".format(question_input))
	df_b4 = ps.sqldf("""SELECT BONUS_Q_4 as Question_Text, BONUS_A_4_1 as Answer_1, BONUS_A_4_2 as Answer_2, BONUS_A_4_3 as Answer_3, BONUS_A_4_4 as Answer_4, 'B4' AS Answer_5, 'B4' AS Answer_6, 'B4' AS Answer_7 
					FROM df_game where BONUS_Q_4 like '%{}%'
					limit 10
					""".format(question_input))

	st.dataframe(pd.concat([df_total, df_b1, df_b2, df_b3, df_b4]))
#################################################################################################################################################
#### THIRD TAB!!! STATS
####
#################################################################################################################################################
with tab3:
	
	st.write("UNDER CONSTRUCTION - be able to look up games and see how those teams and players compare to the rest of the games played")
##----------------------------------------------------------------------------------------------------------------------------------------------------
## find game
##----------------------------------------------------------------------------------------------------------------------------------------------------

	game_find = ""
	team_list = list(df_team.Team.unique())
	clean_name = []
	first_letter = []
	# set up alphabetical order for first letter and team name but removing "The" and "Team"
	for team_name in team_list:
		if team_name[:4] == "The ":
			first_letter.append(team_name[4:5])
			clean_name.append(team_name[4:])
		elif team_name[:5] == "Team ":
			first_letter.append(team_name[5:6])
			clean_name.append(team_name[5:])
		else:
			first_letter.append(team_name[:1])
			clean_name.append(team_name)

	game_dates = df_game[['Season', 'Year', 'Date', 'Game_id']]
	game_dates['Month'] = game_dates.Date.str[:3]
	game_dates['Year_month'] = game_dates.Year.astype(str).str.cat(game_dates.Month, sep='-')
	
	team_table = pd.DataFrame(zip(team_list, first_letter, clean_name), columns = ['Team_name', 'First_letter', 'Clean_name'])
	team_table = team_table.sort_values(by = ['First_letter', 'Clean_name'])
	

	team_or_season = st.selectbox('Select game by Team or Season', options=['select', 'Team', 'Season'])
	

	if team_or_season != 'select' and team_or_season == 'Season':
		season_find = st.selectbox('Select Season', options=['select']+list(game_dates.Season.unique()))
		
		if season_find != 'select':
			month_find = st.selectbox('Select Month', options=['select']+list(game_dates[game_dates.Season == season_find].Year_month.unique()))
	
			if month_find != 'select':
				game_find = st.selectbox('Select Game', options=['select']+list(game_dates[game_dates.Year_month == month_find].Game_id))
	
	
	
	elif team_or_season != 'select' and team_or_season == 'Team':
		team_find = st.selectbox('Select Team Initial', options=['select']+list(team_table.First_letter.unique()))

		if team_find != 'select':
			team_name_find = st.selectbox('Select Team Name', options=['select']+list(team_table[team_table.First_letter == team_find].Team_name.unique()))	 
			
			if team_name_find != 'select':
				game_find = st.selectbox('Select Game', options=['select']+list(df_team[df_team.Team == team_name_find].Game_id))	 
			
		
	st.write(game_find)
	## TODO??? add game id to url so it can be visited within
	st.experimental_set_query_params(game_id = '')
	
	st.experimental_set_query_params(game_id = [game_find])
##----------------------------------------------------------------------------------------------------------------------------------------------------
## build game histogram
##----------------------------------------------------------------------------------------------------------------------------------------------------

	fig = px.histogram(df_team, x="Score_check")
	
	df_specific_game = df_team[df_team.Game_id == game_find][['Team', 'Score_check']]
	team_1 = df_specific_game.iloc[0]
	team_2 = df_specific_game.iloc[1]
	fig.add_vline(x=team_1.Score_check, line_dash="dot", annotation_text=team_1.Team, annotation_position="bottom right", line_color="green")
	
	st.plotly_chart(fig, use_container_width=True)


		
st.write("##")		     
st.text("feedback and questions - america.says.data@gmail.com")
