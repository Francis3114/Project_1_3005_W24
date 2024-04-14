import psycopg as postgre

# Constants
# 
# The following is the connection information for this project. These settings are used to connect PostgreSQL using the appropriate python library.
# We have assumed that the database name would be `Project1` password to be `1234` username to be `postgres` and to be hosted locally.
#

def creat_table(c, con):
    c.execute(''' 
                   CREATE TABLE Competition(
                   competition_id INT PRIMARY KEY,
                   country_name VARCHAR(100),
                   competition_name VARCHAR(100)
                );

                CREATE TABLE Season(
                season_id INT Primary Key,
                    season_name VARCHAR(100)
                );

                Create table Country(
                    country_id INT Primary key,
                    country_name Varchar(100)
                );

                Create table Managers(
                    manager_id int Primary key,
                    name varchar(100),
                    nickname varchar(100),
                    DOB date,
                    country_id int,
                    FOREIGN KEY(country_id) REFERENCES Country(country_id)
                );



                Create table Home_Team(
                    home_team_id INT Primary key,
                    home_team_name Varchar(100),
                    home_team_gender Varchar(100),
                    home_team_group Varchar(100),
                    country_id int,
                    manager_id int,
                    FOREIGN KEY(country_id) REFERENCES Country(country_id),
                    FOREIGN KEY(manager_id) REFERENCES Managers(manager_id)
                );

                Create table Away_Team(
                    away_team_id INT Primary key,
                    away_team_name Varchar(100),
                    away_team_gender Varchar(100),
                    away_team_group Varchar(100),
                    country_id int,
                    manager_id int,
                    FOREIGN KEY(country_id) REFERENCES Country(country_id),
                    FOREIGN KEY(manager_id) REFERENCES Managers(manager_id)
                );

                Create table Competition_Stage(
                    competition_stage_id int Primary key,
                    competition_stage_name Varchar(100)
                );


                Create table Stadium(
                    stadium_id int Primary key,
                    stadium_name Varchar(100),
                    country_id int,
                    FOREIGN KEY(country_id) REFERENCES Country(country_id)
                );

                CREATE TABLE MATCH(
                    match_id INT PRIMARY KEY,
                    match_date date,
                    kick_off time,
                    competition_id INT,
                    season_id INT,
                    home_team_id INT,
                    away_team_id INT,
                    home_score INT,
                    away_score INT,
                    match_status VARCHAR(100),
                    last_updated timestamp,
                    match_week int,
                    competition_stage_id INT,
                    stadium_id INT,
                    FOREIGN KEY(competition_id) REFERENCES competition(competition_id),
                    FOREIGN KEY(season_id) REFERENCES season(season_id),
                    FOREIGN KEY(home_team_id) REFERENCES home_team(home_team_id),
                    FOREIGN KEY(away_team_id) REFERENCES away_team(away_team_id),
                    FOREIGN KEY(competition_stage_id) REFERENCES competition_stage(competition_stage_id),
                    FOREIGN KEY(stadium_id) REFERENCES stadium(stadium_id)
                );
                Create Table Team(
                    team_id INT PRIMARY KEY,
                    team_name VARCHAR(100)
                );
                CREATE Table Player(
                    player_id INT Primary Key,
                    player_name VARCHAR(100),
                    player_nickname VARCHAR(100),
                    jersey_number INT,
                    country_id INT,
                    team_id INT,
                    FOREIGN KEY(country_id) REFERENCES Country(country_id),
                    FOREIGN KEY(team_id) REFERENCES Team(team_id)
                );





                CReate table outcome(
                    outcome_id INT PRIMARY KEY,
                    outcome_name varchar(100)

                );
                create table fifty_fifty(
                    event_id varchar(100) primary key,
                    outcome_id INT,
                    counterpress BOOLEAN DEFAULT false,
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)

                );


                create table card(
                    card_id INT PRIMARY KEY,
                    card_name varchar(100)

                );



                create table bad_behaviour(
                    event_id varchar(100) primary key,
                    card_id INT,
                    FOREIGN KEY(card_id) REFERENCES card(card_id)
                );



                create table ball_receipt(
                    event_id varchar(100) primary key,
                    outcome_id INT,
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                );




                create table ball_recovery(
                    event_id varchar(100) primary key,
                    offensive boolean default false,
                    recovery_failure boolean default false
                );


                create table block(
                    event_id varchar(100) primary key,
                    deflection boolean default false,
                    offensive boolean default false,
                    save_block boolean default false,
                    counterpress boolean default false
                );


                create table dribble(
                    event_id varchar(100) primary key,
                    overrun boolean default false,
                    nutmeg boolean default false,
                    outcome_id int,
                    no_touch boolean default false,
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                );


                create table dribbled_past(
                    event_id varchar(100) primary key,
                    counterpress boolean default false	
                );


                create table type(
                    type_id int primary key,
                    type_name varchar(100)
                );

                create table play_pattern(
                    play_pattern_id int primary key,
                    name varchar(100)
                );

                CREATE Table Events(
                    event_id varchar(100) Primary Key,
                    index int,
                    period int,
                    timestamp time,
                    minute int,
                    second int,
                    type_id int,
                    possession int,
                    possession_team int,
                    play_pattern_id int,
                    team_id INT,
                    player_id INT,
                    match_id int,
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(player_id) REFERENCES player(player_id),
                    FOREIGN KEY(team_id) REFERENCES Team(team_id),
                    FOREIGN KEY(possession_team) REFERENCES Team(team_id),
                    FOREIGN KEY(play_pattern_id) REFERENCES play_pattern(play_pattern_id),
                    FOREIGN KEY(match_id) REFERENCES match(match_id)
                );


                create table duel(
                    event_id varchar(100) primary key,
                    counterpress boolean default false,
                    type_id INT,
                    outcome_id INT,
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table foul_commited(
                    counterpress boolean,
                    offensive boolean,
                    type_id INT,
                    advantage boolean,
                    penalty boolean,
                    card_id int,
                    outcome_id int,
                    event_id varchar(100),
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(card_id) REFERENCES card(card_id),
                    FOREIGN KEY(event_id) REFERENCES events(event_id),
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table technique(
                    technique_id int primary key,
                    technique_name varchar(100)
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table goalkeeper(
                    event_id varchar(100) primary key,
                    technique_id int ,
                    type_id int,
                    outcome_id int,
                    FOREIGN KEY(technique_id) REFERENCES technique(technique_id),
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                    
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table half_end(
                    event_id varchar(100) primary key,
                    early_video_end boolean default false,
                    match_suspended boolean default false
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table half_start(
                    event_id varchar(100) primary key,
                    late_video_start boolean default false
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table injury_stoppage(
                    event_id varchar(100) primary key,
                    in_chain boolean default false
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table interception(
                    event_id varchar(100) primary key,
                    outcome_id int,
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table miscontrol(
                    event_id varchar(100) primary key,
                    aerial_won boolean default false
                    
                );


                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table recipient(
                    recipient_id int,
                    FOREIGN KEY (recipient_id) REFERENCES player(player_id)
                );

                create table height(
                    height_id int primary key,
                    height_name varchar(100)
                );

                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table pass(
                    event_id varchar(100) primary key,
                    recipient_id int,
                    length float,
                    angle float,
                    height_id int,
                    backheel boolean default false,
                    deflected boolean default false,
                    miscommunication boolean default false,
                    crosses boolean default false,
                    cut_back boolean default false,
                    switch boolean default false,
                    shot_assist boolean default false,
                    goal_assist boolean default false,
                    type_id int,
                    outcome_id int,
                    technique_id int,
                    team_id int,
                    match_id int,
                    FOREIGN KEY(recipient_id) REFERENCES player(player_id),
                    FOREIGN KEY(height_id) REFERENCES height(height_id),
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id),
                    FOREIGN KEY(technique_id) REFERENCES technique(technique_id),
                    FOREIGN KEY(team_id) REFERENCES team(team_id),
                    FOREIGN KEY(match_id) REFERENCES match(match_id)
                    
                );




                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table player_off(
                    event_id varchar(100) primary key,
                    permanent boolean default false
                );




                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table pressure(
                    event_id varchar(100) primary key,
                    counterpress boolean default false

                );




                -- FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id)
                create table shot(
                    event_id varchar(100) primary key,
                    key_pass_id varchar(100),
                    aerial_won boolean default false,
                    follows_dribble boolean default false,
                    first_time boolean default false,
                    open_goal boolean default false,
                    statsbomb_xg decimal,
                    deflected boolean default false,
                    technique_id int,
                    type_id int,
                    outcome_id int,
                    team_id int,
                    match_id int,
                    FOREIGN KEY(key_pass_id) REFERENCES pass(event_id),
                    FOREIGN KEY(technique_id) REFERENCES technique(technique_id),
                    FOREIGN KEY(type_id) REFERENCES type(type_id),
                    FOREIGN KEY(outcome_id) REFERENCES outcome(outcome_id),
                    FOREIGN KEY(team_id) REFERENCES team(team_id),
                    FOREIGN KEY(match_id) REFERENCES match(match_id)



                );


                create table substitution(
                    event_id varchar(100) primary key,
                    replacement_id int,
                    outcome_id int,
                    foreign key(replacement_id) references player(player_id),
                    foreign key(outcome_id) references outcome(outcome_id)
                );




                create table formation(
                    formation_id varchar(100),
                    team_id int ,
                    match_id int,
                    foreign key(team_id) references team(team_id),
                    foreign key(match_id) references match(match_id)
                );

                create table starting_XI(
                    match_id int,
                    team_id int,
                    player_id int,
                    foreign key(team_id) references team(team_id),
                    foreign key(match_id) references match(match_id),
                    foreign key(player_id) references player(player_id)

                );
                create table foul_won(
                    event_id varchar(100) primary key,
                    defensive boolean default false,
                    advantage boolean default false,
                    penalty boolean default false
                );

                ''')
    con.commit()