import json
import psycopg as postgre
import os
# Constants

path_to_lineups = os.getcwd() + '/data/lineups'
# 
# The following is the connection information for this project. These settings are used to connect this file to the autograder.
# You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
# For the user "postgres", you must MANUALLY set the password to 1234.
# 

def load_lineup_data(match_id, cursor, conn):
    for i in range(0,(len(match_id))):
        path_to_JSON = path_to_lineups + '\\' + str(match_id[i]) + '.json'
        req = open(path_to_JSON, encoding="utf-8") # without the encoding parameter json.loads won't be able to load the file.
        team_data = json.load(req)
        #try:
        cursor.execute(''' 
                INSERT INTO team (
                team_id,
                team_name)
                VALUES (69420, 'Test_Team_Player' ) ON CONFLICT (team_id) DO NOTHING;'''
                )
        cursor.execute(''' 
                    INSERT INTO country (
                    country_id,
                    country_name)
                    VALUES (1, 'Test_Country') ON CONFLICT (country_id) DO NOTHING;'''
                    )
        cursor.execute(''' 
                        INSERT INTO player (
                            player_id,
                            player_name,
                            player_nickname,
                            jersey_number,
                            country_id,
                            team_id)
                            VALUES (1,'Test Player', 'test nickname',7,1,69420) ON CONFLICT (player_id) DO NOTHING;''',
                                )
                    
        for team in team_data:
            tid = team['team_id']
            tname = team['team_name']
            
            cursor.execute(''' 
                INSERT INTO team (
                team_id,
                team_name)
                VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING;''', (tid, tname)
                )

            
            for j in range(0,(len(team['lineup']))):
                pid = team['lineup'][j]['player_id']
                cid = team['lineup'][j]['country']['id']               
                cursor.execute(''' 
                    INSERT INTO country (
                    country_id,
                    country_name)
                    VALUES (%s, %s) ON CONFLICT (country_id) DO NOTHING;''', (cid, team['lineup'][j]['country']['name'])
                    )

                
               
                cursor.execute(''' 
                                INSERT INTO player (
                                    player_id,
                                    player_name,
                                    player_nickname,
                                    jersey_number,
                                    country_id,
                                    team_id)
                                    VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (player_id) DO NOTHING;''', (
                                        pid,team['lineup'][j]['player_name'],team['lineup'][j]['player_nickname'],team['lineup'][j]['jersey_number'], 
                                        cid, tid)
                                )
    conn.commit()
