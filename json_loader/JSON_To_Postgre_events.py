import json
import psycopg as postgre
import os
# Constants
path_to_events = os.getcwd() + '/data/events'

which_table = {
    42: 'ball_receipt',
    2: 'ball_recovery',
    4: 'duel',
    6: 'block',
    10: 'interception',
    14: 'dribble',
    16: 'shot',
    17: 'pressure',
    18: 'half_start',
    19: 'substitution',
    21: 'foul_won',
    22: 'foul_commited',
    23: 'goalkeeper',
    27: 'player_off',
    24: 'bad_behaviour',
    30: 'pass',
    33: 'fifty_fifty',
    34: 'half_end',
    35: 'starting_XI',
    38: 'miscontrol',
    39: 'dribbled_past',
    40: 'injury_stoppage',
}

def load_events_data(events, cursor, conn):
    for i in range(0,(len(events))):
        path_to_JSON = path_to_events + '\\' + str(events[i]) + '.json'
        req = open(path_to_JSON, encoding="utf-8") # without the encoding parameter json.loads won't be able to load the file.
        event_data = json.load(req)
        #try:
        for event in event_data:
#------------------------------------------------------------------------------Type Table Start-----------------------------------------------------------------------------------------------------

            tid = event['type']['id']
            tname = event['type']['name']
            
            cursor.execute(''' SELECT COUNT(*) FROM type WHERE type_id = %s''', (tid,))
            count_tid = cursor.fetchone()[0]
            if count_tid == 0:
                cursor.execute(''' 
                    INSERT INTO type (
                    type_id,
                    type_name)
                    VALUES (%s, %s)''', (tid, tname)
                    )
            
#------------------------------------------------------------------------------Type Table End-----------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------Play Pattern Start-----------------------------------------------------------------------------------------------------

            play_id = event['play_pattern']['id']
            play_name = event['play_pattern']['name']
            
            cursor.execute(''' SELECT COUNT(*) FROM play_pattern WHERE play_pattern_id = %s''', (play_id,))
            count_tid = cursor.fetchone()[0]
            if count_tid == 0:
                cursor.execute(''' 
                    INSERT INTO play_pattern (
                    play_pattern_id,
                    name)
                    VALUES (%s, %s)''', (play_id, play_name)
                    )
            
#------------------------------------------------------------------------------Play Pattern End-----------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------Events Table Start-----------------------------------------------------------------------------------------------------
            eid = event['id']
            event_index = event['index']
            event_period = event['period']
            type_id = event['type']['id']
            team_id = event['team']['id']
            match_id = events[i]
            try:
                player_id = event['player']['id']                    
            except KeyError:
                # print("Some-freaking-how there is a event but no FREAKING PLAYER!")    
                player_id = 1
                team_id = 69420
                
            cursor.execute(''' INSERT INTO events (event_id, index, period, timestamp, minute, second, type_id, possession, possession_team, play_pattern_id, player_id, team_id, match_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                           (eid, 
                            event_index, 
                            event_period, 
                            event['timestamp'],
                            event['minute'],
                            event['second'],
                            type_id,
                            event['possession'],
                            event['possession_team']['id'], 
                            event['play_pattern']['id'], 
                            player_id,
                            team_id,
                            match_id))
            
            
#------------------------------------------------------------------------------Events Table End-----------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------Subtype Table Start-----------------------------------------------------------------------------------------------------
            match type_id:
            
#------------------------------------------------------------------------------Ball Receipt-----------------------------------------------------------------------------------------------------

                case 42:
                    cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES (8,'Complete') ON CONFLICT (outcome_id) DO NOTHING''')
                    try: 
                        out_id = event['ball_receipt']['outcome']['id']
                        cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({out_id},'{event['ball_receipt']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                    except KeyError:
                        out_id = 8
                        cursor.execute(f''' INSERT INTO {which_table.get(42)} (outcome_id,event_id) VALUES ({out_id},'{event['id']}' )''')
                    cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES (8,'Complete') ON CONFLICT (outcome_id) DO NOTHING''')
                    
#------------------------------------------------------------------------------Ball Recovery----------------------------------------------------------------------------------------------------

                case 2:
                    # print(f"{which_table.get(2)}: Ball Recovery")
                    if('ball_recovery' in event):
                        if('recovery_failure' in event['ball_recovery'] and 'offensive' in event['ball_recovery']):
                            cursor.execute(f''' INSERT INTO {which_table.get(2)} (event_id, offensive,recovery_failure) VALUES ('{event['id']}',{event['ball_recovery']['offensive']},{event['ball_recovery']['recovery_failure']} )''')
                        elif('recovery_failure' in event['ball_recovery'] and not ('offensive' in event['ball_recovery'])):
                            cursor.execute(f''' INSERT INTO {which_table.get(2)} (event_id,recovery_failure) VALUES ('{event['id']}',{event['ball_recovery']['recovery_failure']} )''')
                        else:
                            cursor.execute(f''' INSERT INTO {which_table.get(2)} (event_id,offensive) VALUES ('{event['id']}',{event['ball_recovery']['offensive']} )''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(2)} (event_id) VALUES ('{event['id']}') ''')
                        
#-----------------------------------------------------------------------------Duel-----------------------------------------------------------------------------------------------------

                        
                case 4:
                    if('counterpress' in event):
                        cp = event['counterpress']
                    else:
                        cp = 'false'
                    if('duel' in event):
                        if('type' in event['duel'] and 'outcome' in event['duel']):
                            duel_tid = event['duel']['type']['id']
                            duel_oid = event['duel']['outcome']['id']
                            cursor.execute(f''' INSERT INTO type (type_id,type_name) VALUES ({duel_tid},'{event['duel']['type']['name']}') ON CONFLICT (type_id) DO NOTHING''')
                            cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({duel_oid},'{event['duel']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                            
                        elif('type' in event['duel'] and not 'outcome' in event['duel']):
                            duel_tid = event['duel']['type']['id']
                            duel_oid = 'null'
                            cursor.execute(f''' INSERT INTO type (type_id,type_name) VALUES ({duel_tid},'{event['duel']['type']['name']}') ON CONFLICT (type_id) DO NOTHING''')
                            
                            
                        elif(not 'type' in event['duel'] and  'outcome' in event['duel']):
                            duel_tid =  'null'
                            duel_oid = event['duel']['outcome']['id']
                            cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({duel_oid},'{event['duel']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                            
                        else:
                            duel_tid = 'null'
                            duel_oid = 'null'
                        cursor.execute(f''' INSERT INTO {which_table.get(4)} (event_id,type_id,outcome_id,counterpress) VALUES ('{event['id']}', {duel_tid}, {duel_oid}, {cp}) ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(4)} (event_id,type_id,outcome_id,counterpress) VALUES ('{event['id']}', null, null, {cp}) ''')
                
#------------------------------------------------------------------------------Block-----------------------------------------------------------------------------------------------------

                
                case 6:
                    deff = 'false'
                    off = 'false'
                    sb = 'false'
                    if('counterpress' in event):
                        cp = event['counterpress']
                    else:
                        cp = 'false'
                    if('block' in event):
                        if 'deflection' in event['block'] and 'offensive' in event['block'] and 'save_block' in event['block']:
                            deff = 'true'
                            off = 'true'
                            sb = 'true'
                        elif 'deflection' in event['block'] and 'offensive' in event['block']:
                            deff = 'true'
                            off = 'true'
                            sb = 'false'
                        elif 'deflection' in event['block'] and 'save_block' in event['block']:
                            deff = 'true'
                            off = 'false'
                            sb = 'true'
                        elif 'offensive' in event['block'] and 'save_block' in event['block']:
                            deff = 'false'
                            off = 'true'
                            sb = 'true'
                        elif 'deflection' in event['block']:
                            deff = 'true'
                            off = 'false'
                            sb = 'false'
                        elif 'offensive' in event['block']:
                            deff = 'false'
                            off = 'true'
                            sb = 'false'
                        elif 'save_block' in event['block']:
                            deff = 'false'
                            off = 'false'
                            sb = 'true'
                        cursor.execute(f''' INSERT INTO {which_table.get(6)} (event_id,deflection,offensive,save_block,counterpress) VALUES ('{event['id']}', {deff}, {off}, {sb}, {cp}) ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(6)} (event_id,counterpress) VALUES ('{event['id']}',{cp}) ''')
                
#------------------------------------------------------------------------------Interception-----------------------------------------------------------------------------------------------------

                
                case 10:
                    cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({event['interception']['outcome']['id']},'{event['interception']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                    if('interception' in event):
                        if('outcome' in event['interception']):
                            cursor.execute(f''' INSERT INTO {which_table.get(10)} (event_id,outcome_id) VALUES ('{event['id']}',{event['interception']['outcome']['id']}) ''')
                        else:
                            cursor.execute(f''' INSERT INTO {which_table.get(10)} (event_id) VALUES ('{event['id']}') ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(10)} (event_id) VALUES ('{event['id']}') ''')
                        
#------------------------------------------------------------------------------Dribble-----------------------------------------------------------------------------------------------------


                case 14:
                    overrun = 'false'
                    nutmeg = 'false'
                    no_touch = 'false'
                    outcome_id = 'NULL'
                    
                    if('dribble' in event):
                        if 'overrun' in event['dribble']:
                            overrun = str(event['dribble']['overrun']).lower()
                        
                        if 'nutmeg' in event['dribble']:
                            nutmeg = str(event['dribble']['nutmeg']).lower()
                        
                        if 'no_touch' in event['dribble']:
                            no_touch = str(event['dribble']['no_touch']).lower()
                        
                        if 'outcome' in event['dribble']:
                            outcome_id = event['dribble']['outcome']['id']
                            cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({outcome_id},'{event['dribble']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                            
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(14)} (event_id, overrun, nutmeg, no_touch, outcome_id) 
                            VALUES ('{event['id']}', {overrun}, {nutmeg}, {no_touch}, {outcome_id}) ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(14)} (event_id) VALUES ('{event['id']}') ''')

#------------------------------------------------------------------------------Shot-----------------------------------------------------------------------------------------------------
             
                        
                case 16:
                    aerial_won = 'false'
                    follows_dribble = 'false'
                    first_time = 'false'
                    open_goal = 'false'
                    deflected = 'false'
                    statsbomb_xg = 'NULL'
                    key_pass_id = 'NULL'
                    outcome_id = 'NULL'
                    type_id = 'NULL'
                    technique_id = 'NULL'
                    team_id = event['team']['id']
                    match_id = events[i]
                    if('shot' in event):
                        if 'aerial_won' in event['shot']:
                            aerial_won = str(event['shot']['aerial_won']).lower()
                        
                        if 'follows_dribble' in event['shot']:
                            follows_dribble = str(event['shot']['follows_dribble']).lower()
                        
                        if 'first_time' in event['shot']:
                            first_time = str(event['shot']['first_time']).lower()
                        
                        if 'open_goal' in event['shot']:
                            open_goal = str(event['shot']['open_goal']).lower()
                        
                        if 'deflected' in event['shot']:
                            deflected = str(event['shot']['deflected']).lower()
                        
                        if 'statsbomb_xg' in event['shot']:
                            statsbomb_xg = event['shot']['statsbomb_xg']
                        
                        if 'key_pass_id' in event['shot']:
                            key_pass_id = event['shot']['key_pass_id']
                        
                        if 'outcome' in event['shot']:
                            outcome_id = event['shot']['outcome']['id']
                            cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({outcome_id},'{event['shot']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                            
                            
                        
                        if 'type' in event['shot']:
                            type_id = event['shot']['type']['id']
                            cursor.execute(f''' INSERT INTO type (type_id,type_name) VALUES ({type_id},'{event['shot']['type']['name']}') ON CONFLICT (type_id) DO NOTHING''')
                            
                            
                        
                        if 'technique' in event['shot']:
                            technique_id = event['shot']['technique']['id']
                            cursor.execute(f''' INSERT INTO technique (technique_id,technique_name) VALUES ({technique_id},'{event['shot']['technique']['name']}') ON CONFLICT (technique_id) DO NOTHING''')
                            
                        if key_pass_id == 'NULL':
                            cursor.execute(f''' 
                            INSERT INTO {which_table.get(16)} (event_id, aerial_won, follows_dribble, first_time, open_goal, deflected, statsbomb_xg, key_pass_id, outcome_id, type_id, technique_id, team_id, match_id) 
                            VALUES ('{event['id']}', {aerial_won}, {follows_dribble}, {first_time}, {open_goal}, {deflected}, {statsbomb_xg}, {key_pass_id}, {outcome_id}, {type_id}, {technique_id}, {team_id}, {match_id}) ''')
                        else:
                            cursor.execute(f''' 
                            INSERT INTO {which_table.get(16)} (event_id, aerial_won, follows_dribble, first_time, open_goal, deflected, statsbomb_xg, key_pass_id, outcome_id, type_id, technique_id, team_id, match_id) 
                            VALUES ('{event['id']}', {aerial_won}, {follows_dribble}, {first_time}, {open_goal}, {deflected}, {statsbomb_xg}, '{key_pass_id}', {outcome_id}, {type_id}, {technique_id}, {team_id}, {match_id}) ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(16)} (event_id) VALUES ('{event['id']}') ''')
                        
#------------------------------------------------------------------------------Pressure-----------------------------------------------------------------------------------------------------


                case 17:
                    if('counterpress' in event):
                        cp = event['counterpress']
                    else:
                        cp = 'false'
                    
                    cursor.execute(f''' INSERT INTO {which_table.get(17)} (event_id,counterpress) VALUES ('{event['id']}',{cp}) ''')
                    
#------------------------------------------------------------------------------Half_Start----------------------------------------------------------------------------------------------------

                    
                case 18:
                    try:
                        if('late_video_start' in event or 'late video start' in event):
                            try:
                                lvs = event['late_video_start']
                            except KeyError:
                                lvs = event['late video start']
                        else:
                            lvs = 'false'
                        
                        
                        if('late_video_start' in event['half_start'] or 'late video start' in event['half_start']):
                            lvs = event['half_start']['late_video_start']
                            try:
                                lvs = event['half_start']['late_video_start']
                            except KeyError:
                                lvs = event['half_start']['late video start']
                        else:
                            lvs = 'false'
                        cursor.execute(f''' INSERT INTO {which_table.get(18)} (event_id,late_video_start) VALUES ('{event['id']}',{lvs}) ''')
                    except KeyError:
                        cursor.execute(f''' INSERT INTO {which_table.get(18)} (event_id,late_video_start) VALUES ('{event['id']}',false) ''')

#------------------------------------------------------------------------------Substitution-----------------------------------------------------------------------------------------------------

                case 19:
                    outcome_id = 'NULL'
                    replacement_id = 'NULL'
                    if('substitution' in event):
                        if 'outcome' in event['substitution']:
                            outcome_id = event['substitution']['outcome']['id']
                            cursor.execute(f''' INSERT INTO outcome (outcome_id,outcome_name) VALUES ({outcome_id},'{event['substitution']['outcome']['name']}') ON CONFLICT (outcome_id) DO NOTHING''')
                        
                        if 'replacement' in event['substitution']:
                            replacement_id = event['substitution']['replacement']['id']
                        
                        
                        
                        cursor.execute(f''' 
                        INSERT INTO {which_table.get(19)} (event_id, outcome_id, replacement_id) 
                        VALUES ('{event['id']}', {outcome_id}, {replacement_id}) ''')
                        
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(19)} (event_id) VALUES ('{event['id']}') ''')
                        
        
#------------------------------------------------------------------------------Foul_won-----------------------------------------------------------------------------------------------------

                    
                    
                case 21:
                    defensive = 'false'
                    advantage = 'false'
                    penalty = 'false'
                    if('foul_won' in event):
                        if 'defensive' in event['foul_won']:
                            defensive = str(event['foul_won']['defensive']).lower()
                        
                        if 'advantage' in event['foul_won']:
                            advantage = str(event['foul_won']['advantage']).lower()
                        
                        if 'penalty' in event['foul_won']:
                            penalty = str(event['foul_won']['penalty']).lower()
                            
           
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(21)} (event_id, defensive, advantage, penalty) 
                            VALUES ('{event['id']}', {defensive}, {advantage}, {penalty}) ''')
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(21)} (event_id) VALUES ('{event['id']}') ''')

#------------------------------------------------------------------------------Shot-----------------------------------------------------------------------------------------------------

                case 22:
                    counterpress = 'false'
                    offensive = 'false'
                    advantage = 'false'
                    penalty = 'false'
                    outcome_id = 'NULL'
                    card_id = 'NULL'
                    try:
                        if 'counterpress' in event['foul_committed']:
                                counterpress = str(event['foul_committed']['counterpress']).lower()
                    except KeyError:
                        counterpress = 'false'
                    if('foul_committed' in event):   
                        if 'offensive' in event['foul_committed']:
                            offensive = str(event['foul_committed']['offensive']).lower()
                        
                        if 'advantage' in event['foul_committed']:
                            advantage = str(event['foul_committed']['advantage']).lower()
                        
                        if 'penalty' in event['foul_committed']:
                            penalty = str(event['foul_committed']['penalty']).lower()
                        
                        if 'outcome' in event['foul_committed']:
                            outcome_id = event['foul_committed']['outcome']['id']
                            cursor.execute("INSERT INTO Outcome (outcome_id, outcome_name) VALUES (%s, %s) ON CONFLICT (outcome_id) DO NOTHING", (outcome_id, event['foul_committed']['outcome']['name']))
                            
                        
                        if 'card' in event['foul_committed']:
                            card_id = event['foul_committed']['card']['id']
                            cursor.execute("INSERT INTO Card (card_id, card_name) VALUES (%s, %s) ON CONFLICT (card_id) DO NOTHING", (card_id, event['foul_committed']['card']['name']))
                        
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(22)} (event_id, counterpress, offensive, advantage, penalty, outcome_id, card_id) 
                            VALUES ('{event['id']}', {counterpress}, {offensive}, {advantage}, {penalty}, {outcome_id}, {card_id}) ''')
                        

                    else:
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(22)} (event_id, outcome_id, card_id) 
                            VALUES ('{event['id']}', {outcome_id}, {card_id}) ''')
                        
#------------------------------------------------------------------------------Goalkeeper-----------------------------------------------------------------------------------------------------


                case 23:
                    technique_id = 'NULL'
                    type_id = 'NULL'
                    outcome_id = 'NULL'
                    if('goalkeeper' in event):
                        if 'technique' in event['goalkeeper']:
                            technique_id = event['goalkeeper']['technique']['id']
                            cursor.execute("INSERT INTO Technique (technique_id, technique_name) VALUES (%s, %s) ON CONFLICT (technique_id) DO NOTHING", (technique_id, event['goalkeeper']['technique']['name']))
                        
                        if 'type' in event['goalkeeper']:
                            type_id = event['goalkeeper']['type']['id']
                            cursor.execute("INSERT INTO Type (type_id, type_name) VALUES (%s, %s) ON CONFLICT (type_id) DO NOTHING", (type_id, event['goalkeeper']['type']['name']))
                        
                        if 'outcome' in event['goalkeeper']:
                            outcome_id = event['goalkeeper']['outcome']['id']
                            cursor.execute("INSERT INTO Outcome (outcome_id, outcome_name) VALUES (%s, %s) ON CONFLICT (outcome_id) DO NOTHING", (outcome_id, event['goalkeeper']['outcome']['name']))
                        
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(23)} (event_id, technique_id, type_id, outcome_id) 
                            VALUES ('{event['id']}', {technique_id}, {type_id}, {outcome_id}) ''')

                    else:
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(23)} (event_id, technique_id, type_id, outcome_id) 
                            VALUES ('{event['id']}', {technique_id}, {type_id}, {outcome_id}) ''')

#------------------------------------------------------------------------------PLlayer_off-----------------------------------------------------------------------------------------------------


                case 27:
                    p = 'false'
                    if('permanent' in event):
                        p = event['permanent']
                        cursor.execute(f''' INSERT INTO {which_table.get(27)} (event_id,permanent) VALUES ('{event['id']}',{p}) ''')
                    else:
                        p = 'false'
                        try:
                            if('permanent' in event['player_off']):
                                p = event['player_off']['permanent']
                            else:
                                p = 'false'
                            cursor.execute(f''' INSERT INTO {which_table.get(27)} (event_id,permanent) VALUES ('{event['id']}',{p}) ''')
                        except KeyError:
                            cursor.execute(f''' INSERT INTO {which_table.get(27)} (event_id,permanent) VALUES ('{event['id']}',{p}) ''')
                    
                    
#------------------------------------------------------------------------------Bad_Behaviour-----------------------------------------------------------------------------------------------------

                    
                case 24:
                    card_id = 'NULL'
                    if('bad_behaviour' in event):
                        if 'card' in event['bad_behaviour']:
                            card_id = event['bad_behaviour']['card']['id']
                            cursor.execute("INSERT INTO Card (card_id, card_name) VALUES (%s, %s) ON CONFLICT (card_id) DO NOTHING", (card_id, event['bad_behaviour']['card']['name']))
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(24)} (event_id,card_id) VALUES ('{event['id']}',{card_id}) ''')
                        
#------------------------------------------------------------------------------Pass-----------------------------------------------------------------------------------------------------

                        
                case 30:
                    backheel = 'false'
                    deflected = 'false'
                    miscommunication = 'false'
                    cross = 'false'
                    cut_back = 'false'
                    switch = 'false'
                    shot_assist = 'false'
                    goal_assist = 'false'
                    length = 0
                    angle = 0
                    recipient_id = 'null'
                    height_id = 'null'
                    outcome_id = 'null'
                    type_id = 'null'
                    technique_id = 'null'
                    match_id = events[i]
                    team_id = event['team']['id']
                    if('pass' in event):
                        
                        if 'backheel' in event['pass']:
                            backheel = str(event['pass']['backheel']).lower()
                        
                        if 'deflected' in event['pass']:
                            deflected = str(event['pass']['deflected']).lower()
                        
                        if 'miscommunication' in event['pass']:
                            miscommunication = str(event['pass']['miscommunication']).lower()
                        
                        if 'cross' in event['pass']:
                            cross = str(event['pass']['cross']).lower()
                        
                        if 'cut_back' in event['pass']:
                            cut_back = str(event['pass']['cut_back']).lower()
                        
                        if 'switch' in event['pass']:
                            switch = str(event['pass']['switch']).lower()
                        
                        if 'shot_assist' in event['pass']:
                            shot_assist = str(event['pass']['shot_assist']).lower()
                        
                        if 'goal_assist' in event['pass']:
                            goal_assist = str(event['pass']['goal_assist']).lower()
                        
                        if 'length' in event['pass']:
                            length = event['pass']['length']
                        
                        if 'angle' in event['pass']:
                            angle = event['pass']['angle']
                        
                        if 'recipient' in event['pass']:
                            recipient_id = event['pass']['recipient']['id']
                            cursor.execute(f"INSERT INTO Recipient (recipient_id) VALUES ({recipient_id})")
                        
                        if 'height' in event['pass']:
                            height_id = event['pass']['height']['id']
                            cursor.execute("INSERT INTO Height (height_id, height_name) VALUES (%s, %s) ON CONFLICT (height_id) DO NOTHING", (height_id, event['pass']['height']['name']))
                        
                        if 'outcome' in event['pass']:
                            outcome_id = event['pass']['outcome']['id']
                            cursor.execute("INSERT INTO Outcome (outcome_id, outcome_name) VALUES (%s, %s) ON CONFLICT (outcome_id) DO NOTHING", (outcome_id, event['pass']['outcome']['name']))

                        
                        if 'type' in event['pass']:
                            type_id = event['pass']['type']['id']
                            cursor.execute("INSERT INTO Type (type_id, type_name) VALUES (%s, %s) ON CONFLICT (type_id) DO NOTHING", (type_id, event['pass']['type']['name']))
                        
                        if 'technique' in event['pass']:
                            technique_id = event['pass']['technique']['id']
                            cursor.execute("INSERT INTO Technique (technique_id, technique_name) VALUES (%s, %s) ON CONFLICT (technique_id) DO NOTHING", (technique_id, event['pass']['technique']['name']))
                    
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(30)} (event_id, backheel, deflected, miscommunication, crosses, cut_back, switch, shot_assist, goal_assist, length, angle, recipient_id, height_id, outcome_id, type_id, technique_id, match_id, team_id) 
                            VALUES ('{event['id']}', {backheel}, {deflected}, {miscommunication}, {cross}, {cut_back}, {switch}, {shot_assist}, {goal_assist}, {length}, {angle}, {recipient_id}, {height_id}, {outcome_id}, {type_id}, {technique_id}, {match_id}, {team_id}) ''')

                    else:
                        cursor.execute(f''' 
                            INSERT INTO {which_table.get(30)} (event_id, length, angle, recipient_id, height_id, outcome_id, type_id, technique_id, match_id, team_id) 
                            VALUES ('{event['id']}', {length}, {angle}, {recipient_id}, {height_id}, {outcome_id}, {type_id}, {technique_id}, {match_id}, {team_id}) ''')

#------------------------------------------------------------------------------50-50-----------------------------------------------------------------------------------------------------


                case 33:
                    out_id = 'null'
                    if('counterpress' in event):
                        cp = event['counterpress']
                    else:
                        cp = 'false'
                    if('50_50' in event):
                        if('outcome' in event['50_50']):
                            
                            out_id = event['50_50']['outcome']['id']
                            cursor.execute("INSERT INTO outcome (outcome_id, outcome_name) VALUES (%s, %s) ON CONFLICT (outcome_id) DO NOTHING", (out_id, event['50_50']['outcome']['name']))
                            cursor.execute(f''' INSERT INTO {which_table.get(33)} (event_id,counterpress, outcome_id) VALUES ('{event['id']}',{cp}, {out_id}) ''')
                            
                    else:
                        cursor.execute(f''' INSERT INTO {which_table.get(33)} (event_id, outcome_id) VALUES ('{event['id']}', {out_id}) ''') 
                
#------------------------------------------------------------------------------Half_End-----------------------------------------------------------------------------------------------------

                
                case 34:
                    try:
                        if('early_video_end' in event or 'early video end' in event):
                            try:
                                evs = event['early_video_end']
                            except KeyError:
                                evs = event['early video end']
                        else:
                            evs = 'false'
                        
                        if('early_video_end' in event['half_end'] or 'early video end' in event['half_end']):
                            try:
                                evs = event['half_end']['early_video_start']
                            except KeyError:
                                evs = event['half_end']['early video end']
                        else:
                            evs = 'false'
                        
                        cursor.execute(f''' INSERT INTO {which_table.get(18)} (event_id,late_video_start) VALUES ('{event['id']}',{evs}) ''')
                    except KeyError:
                        cursor.execute(f''' INSERT INTO {which_table.get(18)} (event_id,late_video_start) VALUES ('{event['id']}',false) ''')
#------------------------------------------------------------------------------Starting XI-----------------------------------------------------------------------------------------------------

                
                case 35:
                    lineup = event['tactics']['lineup']
                    player_ids = [player['player']['id'] for player in lineup]
                    
                    for player_id in player_ids:
                        cursor.execute(f"INSERT INTO {which_table.get(35)} (team_id, match_id, player_id) VALUES ({event['team']['id']}, {events[i]}, {player_id})")
                        
#------------------------------------------------------------------------------Miscontrol-----------------------------------------------------------------------------------------------------

                        
                case 38:
                    awon = 'false'
                    if('miscontrol' in event):
                        awon = event['miscontrol']['aerial_won']
                    else:
                        awon = 'false'
                    cursor.execute(f"INSERT INTO {which_table.get(38)} (event_id, aerial_won) VALUES ('{event['id']}', {awon})")
                    
#------------------------------------------------------------------------------dribbled_past----------------------------------------------------------------------------------------------------

                    
                case 39:
                    if('counterpress' in event):
                        cp = event['counterpress']
                    else:
                        cp = 'false'
                    cursor.execute(f''' INSERT INTO {which_table.get(39)} (event_id,counterpress) VALUES ('{event['id']}',{cp}) ''')
                    
#------------------------------------------------------------------------------iNJURY STOPPAGE-----------------------------------------------------------------------------------------------------

                    
                case 40:
                    # outcome_id = 'null'
                    outcome_id = 'false'
                    try:
                        if('in_chain' in event['injury_stoppage'] ):
                            outcome_id = event['injury_stoppage']['in_chain']

                        else:
                            outcome_id = 'false'
                        
                        cursor.execute(f''' INSERT INTO {which_table.get(40)} (event_id,in_chain) VALUES ('{event['id']}',{outcome_id}) ''')
                    except KeyError:
                        cursor.execute(f''' INSERT INTO {which_table.get(40)} (event_id,in_chain) VALUES ('{event['id']}',{outcome_id}) ''')
                case _:
                    count =0
                    # print(f'Coulld not resolve the event ({event['type']['name']}) with ID: {event['type']['id']}' )
#------------------------------------------------------------------------------Subtype Table End-----------------------------------------------------------------------------------------------------



    conn.commit()
