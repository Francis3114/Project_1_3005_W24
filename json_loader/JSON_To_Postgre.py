import json
import psycopg as postgre
from JSON_To_Postgre_lineups import load_lineup_data
from JSON_To_Postgre_events import load_events_data
from table_creator import creat_table
import time
import os
# Constants
# Constants
# 
# The following is the connection information for this project. These settings are used to connect PostgreSQL using the appropriate python library.
# We have assumed that the database name would be `Project1` password to be `1234` username to be `postgres` and to be hosted locally.
#
conn = postgre.connect(dbname="Project1",
                        host="localhost",
                        user="postgres",
                        password="1234",
                        port="5432")

cursor = conn.cursor()
ptm = os.getcwd() + '/data/matches'
desired_competition_id = []
desired_season_id = []


creat_table(cursor, conn)


with open('competitions.json') as comp:
   comp_data = json.load(comp)
   
competition_list = [
    {'competition_name': 'La Liga', 'season_name': '2018/2019'},
    {'competition_name': 'La Liga', 'season_name': '2019/2020'},
    {'competition_name': 'La Liga', 'season_name': '2020/2021'},
    {'competition_name': 'Premier League', 'season_name': '2003/2004'}
]
    
for item in comp_data:
    for comp in competition_list:
        if item['competition_name'] == comp['competition_name'] and item['season_name'] == comp['season_name']:
            desired_competition_id.append(item['competition_id'])
            desired_season_id.append(item['season_id'])

curtime = time.time()           
for i in range(0,4):
    desired_match_id = []

    path_to_JSON = ptm + '\\' + str(desired_competition_id[i]) + '\\' + str(desired_season_id[i]) + '.json'
    req = open(path_to_JSON, encoding="utf-8") # without the encoding parameter json.loads won't be able to load the file.
    matches_data = json.load(req)
    
    for match in matches_data:
        mid = match['match_id']
        desired_match_id.append(mid)
        md = match['match_date']
        ko = match['kick_off']
        hs = match['home_score']
        aws = match['away_score']
        ms = match['match_status']
        lu = match['last_updated']
        mw = match['match_week']
        cid = match['competition']['competition_id'] 

        cursor.execute('''SELECT COUNT(*) FROM competition WHERE competition_id = %s''', (cid,))
        count = cursor.fetchone()[0]
        if count == 0:
            cursor.execute(''' 
                        INSERT INTO competition (
                            competition_id,
                            country_name,
                            competition_name)
                            VALUES (%s, %s, %s)''', (
                                cid,match['competition']['country_name'], 
                                match['competition']['competition_name'])
                        )
        conn.commit()
        
        
        sid = match['season']['season_id']
        cursor.execute('''SELECT COUNT(*) FROM season WHERE season_id = %s''', (sid,))
        count = cursor.fetchone()[0]
        if count == 0:
            cursor.execute(''' 
                        INSERT INTO season (
                            season_id,
                            season_name)
                            VALUES (%s, %s)''', (
                                match['season']['season_id'], 
                                match['season']['season_name'])
                        )
        conn.commit()
        
#------------------------------------------------------------------------------------------------------------

        country_homeID = match['home_team']['country']['id']
        cursor.execute('''SELECT COUNT(*) FROM country WHERE country_id = %s''', (country_homeID,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute(''' 
                        INSERT INTO country (
                            country_id,
                            country_name)
                            VALUES (%s, %s)''', (
                                country_homeID, 
                                match['home_team']['country']['name'])
                        )
        conn.commit()
        try:
            manager_country_homeID = match['home_team']['managers'][0]['country']['id']
            cursor.execute('''SELECT COUNT(*) FROM country WHERE country_id = %s''', (manager_country_homeID,))
            count = cursor.fetchone()[0]
        
            if count == 0:
                cursor.execute(''' 
                            INSERT INTO country (
                                country_id,
                                country_name)
                                VALUES (%s, %s)''', (
                                    manager_country_homeID, 
                                    match['home_team']['managers'][0]['country']['name'])
                            )
            conn.commit()
            
            manager_homeid = match['home_team']['managers'][0]['id']
            cursor.execute('''SELECT COUNT(*) FROM managers WHERE manager_id = %s''', (manager_homeid,))
            count = cursor.fetchone()[0]
        
            if count == 0:
                cursor.execute(''' 
                            INSERT INTO managers (
                                manager_id,
                                name,
                                nickname,
                                dob,
                                country_id)
                                VALUES (%s, %s, %s , %s, %s)''', (
                                    manager_homeid,
                                    match['home_team']['managers'][0]['name'],
                                    match['home_team']['managers'][0]['nickname'],
                                    match['home_team']['managers'][0]['dob'],
                                    match['home_team']['managers'][0]['country']['id']
                                    )
                            )
            conn.commit()
        except KeyError:
            count = 0
               
        
        home_teamID = match['home_team']['home_team_id']
        cursor.execute('''SELECT COUNT(*) FROM home_team WHERE home_team_id = %s''', (home_teamID,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute(''' 
                        INSERT INTO home_team (
                            home_team_id,
                            home_team_name,
                            home_team_gender,
                            home_team_group,
                            country_id,
                            manager_id)
                            VALUES (%s, %s,%s, %s , %s , %s)''', (
                                match['home_team']['home_team_id'], 
                                match['home_team']['home_team_name'],
                                match['home_team']['home_team_gender'], 
                                match['home_team']['home_team_group'],
                                country_homeID,
                                manager_homeid
                                )
                        )
        conn.commit()
#------------------------------------------------------------------------------------------------------------
        
        country_awayID = match['away_team']['country']['id']
        
        cursor.execute('''SELECT COUNT(*) FROM country WHERE country_id = %s''', (country_awayID,))
        count = cursor.fetchone()[0]
        
        if count == 0:
                    cursor.execute(''' 
                       INSERT INTO country (
                           country_id,
                           country_name)
                        VALUES (%s, %s)''', (
                            country_awayID, 
                            match['away_team']['country']['name'])
                       )
        conn.commit()
        
        
        try:
            manager_country_awayID = match['away_team']['managers'][0]['country']['id']
        
            cursor.execute('''SELECT COUNT(*) FROM country WHERE country_id = %s''', (manager_country_awayID,))
            count = cursor.fetchone()[0]
        
            if count == 0:
                cursor.execute(''' 
                            INSERT INTO country (
                                country_id,
                                country_name)
                                VALUES (%s, %s)''', (
                                    manager_country_awayID, 
                                    match['away_team']['managers'][0]['country']['name'])
                            )
            conn.commit()
            
            manager_awayID = match['away_team']['managers'][0]['id']
            
            cursor.execute('''SELECT COUNT(*) FROM managers WHERE manager_id = %s''', (manager_awayID,))
            count = cursor.fetchone()[0]
            
            if count == 0:
                cursor.execute(''' 
                        INSERT INTO managers (
                            manager_id,
                            name,
                            nickname,
                            dob,
                            country_id)
                            VALUES (%s, %s, %s , %s, %s)''', (
                                manager_awayID,
                                match['away_team']['managers'][0]['name'],
                                match['away_team']['managers'][0]['nickname'],
                                match['away_team']['managers'][0]['dob'],
                                match['away_team']['managers'][0]['country']['id']
                                )
                        )
            conn.commit()
            
        except KeyError:
            count = 0
            
            
        away_teamID = match['away_team']['away_team_id']
        cursor.execute('''SELECT COUNT(*) FROM away_team WHERE away_team_id = %s''', (away_teamID,))
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute(''' 
                        INSERT INTO away_team (
                            away_team_id,
                            away_team_name,
                            away_team_gender,
                            away_team_group,
                            country_id,
                            manager_id)
                            VALUES (%s, %s,%s, %s , %s , %s)''', (
                                match['away_team']['away_team_id'], 
                                match['away_team']['away_team_name'],
                                match['away_team']['away_team_gender'], 
                                match['away_team']['away_team_group'],
                                country_awayID,
                                manager_awayID
                                )
                        )
        conn.commit()
    
#------------------------------------------------------------------------------------------------------------

        comp_stageID = match['competition_stage']['id']
        
        cursor.execute('''SELECT COUNT(*) FROM competition_stage WHERE competition_stage_id = %s''', (comp_stageID,))
        count = cursor.fetchone()[0]
        
        if count == 0:
             cursor.execute(''' 
                       INSERT INTO competition_stage (
                           competition_stage_id,
                           competition_stage_name)
                        VALUES (%s, %s)''', (
                            comp_stageID, 
                            match['competition_stage']['name'])
                       )
        conn.commit()
        
#------------------------------------------------------------------------------------------------------------
        try:
            
            stadium_id = match['stadium']['id']
            
            cursor.execute('''SELECT COUNT(*) FROM STADIUM WHERE stadium_id = %s''', (stadium_id,))
            count = cursor.fetchone()[0]
            
            if count == 0:
                cursor.execute(''' 
                        INSERT INTO stadium (
                            stadium_id,
                            stadium_name,
                            country_id)
                            VALUES (%s, %s, %s)''', (
                                stadium_id, 
                                match['stadium']['name'],
                                match['stadium']['country']['id'])
                        )
            conn.commit()
        except KeyError:
            count = 0
        
#------------------------------------------------------------------------------------------------------------




        cursor.execute('''
                    INSERT INTO match(
                        match_id,
                        match_date,
                        kick_off,
                        competition_id,
                        season_id,
                        home_team_id,
                        away_team_id,
                        home_score,
                        away_score,
                        match_status,
                        last_updated,
                        match_week,
                        competition_stage_id,
                        stadium_id
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (match_id) DO NOTHING''',(
                        mid,
                        md,
                        ko,
                        cid,
                        sid,
                        home_teamID,
                        away_teamID,
                        hs,
                        aws,
                        ms,
                        lu,
                        mw,
                        comp_stageID,
                        stadium_id

                    
                    )
        )

    load_lineup_data(desired_match_id, cursor, conn)
    load_events_data(desired_match_id, cursor, conn)
    
conn.commit()

cursor.close()
conn.close()

print((time.time()-curtime)/60)
