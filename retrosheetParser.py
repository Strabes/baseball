import numpy as np
import pandas as pd
import json
import pymongo
import re
from itertools import cycle
import glob
import os
from more_itertools import split_before

def RetrosheetEventFormatter(lst,incComInPlays = True):
    '''
    Function for ingesting a single game in Retrosheet event format
    and creating a dictionary from it that contains pandas DataFrames for:
    1. info
    2. plays
    3. starters
    4. subs
    '''
    d = {}
    ID = lst[0].split(",")
    if ID[0] == 'id':
        d['id'] = ID[1]
    else:
        raise ValueError("First field must be game ID")
    d['info'] = (
        pd.DataFrame(
            [j.split(",")[1:] for j in lst if j.startswith("info,")],
            columns = ['Key','Value'])
    )
    
    playsAndComments = [j for j in lst if re.match("((play)|(com)),",j) is not None]
    t = 0
    for i,j in enumerate(playsAndComments):
        if re.match("play,",j):
            playsAndComments[i] = j.split(",")
            playsAndComments[i].append("")
            t = 1
        elif re.match("com,",j):
            playsAndComments[i] = ['com'] + ['']*6 + [j.split(",")[1].strip('"').strip('$ ')]
            if t >= 0:
                playsAndComments[i-t][7] = (playsAndComments[i-t][7] + " " + playsAndComments[i][7]).strip(" ")
                t+=1
        
    d['plays'] = (
        pd.DataFrame(
            #[j.split(",")[1:] for j in lst if j.startswith("play,")],
            [j for j in playsAndComments if j[0] == 'play'],
            columns = ["Type","Inning","Team","PlayerID","Count","PitchSequence","Play","Comment"])
    )
    d['starters'] = (
        pd.DataFrame(
            [j.split(",")[1:] for j in lst if j.startswith("start,")],
            columns = ["PlayerID","PlayerName","Team","BattingPosition","FieldingPosition"])
    )
    i = -1
    subs = []
    for j in lst:
        if j.startswith("sub,"):
            sub = j.split(",")
            sub.append(i)
            subs.append(sub[1:])
        elif j.startswith("play,"):
            i += 1
    
    d['subs'] = (pd.DataFrame(
                subs,
                columns = ["PlayerID","PlayerName","Team","BattingPosition",
                           "FieldingPosition","SubbedAfterPlayNum"]
    ))
    
    return(d)


def split_sep_rspct_paren(sentence,separators="\/|\."):
    '''
    Function for splitting strings on 'separators' while respecting
    balanced parentheses
    
    Parameters
    -------------
    sentence : string to be split
    separators : regular expression to use as separators
    
    Returns
    -------------
    list : list of substrings

    '''
    nb_brackets=0
    sentence = (
        re.compile(
            "(^(" + separators + "))|((" + separators + ")$)"
        ).sub("",sentence))
    
    l=[0]
    for i,c in enumerate(sentence):
        if c=="(":
            nb_brackets+=1
        elif c==")":
            nb_brackets-=1
        elif re.match(separators,c) is not None and nb_brackets==0:
            l.append(i)
        # handle malformed string
        if nb_brackets<0:
            raise Exception("Syntax error")

    l.append(len(sentence))
    # handle missing closing parentheses
    if nb_brackets>0:
        raise Exception("Syntax error")

    return([sentence[i:j] for i,j in zip(l,l[1:])])


def _event_splitter(sentence):
    '''
    Function for splitting a string upon reaching a balanced, closing parenthesis
    when the next character is not an opening parenthesis
    
    Parameters
    -------------
    sentence : string to be split
    
    Returns
    -------------
    list : list of substrings
    
    Example
    -------------
    '46(1)(E6)63' -> ['46(1)(E6)','63']
    '''
    nb_brackets=0
    
    prev_char_right_par = False
    l=[0]
    for i,c in enumerate(sentence):
        if c=="(":
            nb_brackets+=1
        elif c==")":
            nb_brackets-=1
        elif prev_char_right_par and nb_brackets==0:
            l.append(i)
        if c==")":
            prev_char_right_par = True
        else:
            prev_char_right_par = False
        # handle malformed string
        if nb_brackets<0:
            raise Exception("Syntax error")

    l.append(len(sentence))
    # handle missing closing parentheses
    if nb_brackets>0:
        raise Exception("Syntax error")

    return([sentence[i:j] for i,j in zip(l,l[1:])])

def _subevent_splitter(event):
    '''
    Function for splitting an event into subevents, first splitting
    on '+' and then splitting upon reaching a balanced, closing parenthesis
    when the next character is not an opening parenthesis
    
    Parameters
    -------------
    event : event string to be split into subevent
    
    Returns
    -------------
    list : list of subevents
    
    Example
    -------------
    '46(1)(E6)63' -> ['46(1)(E6)','63']
    '''
    splits = split_sep_rspct_paren(event,separators='\+')
    subevent_list = [i.strip('+') for j in splits for i in _event_splitter(j)]
    return(subevent_list)

def _playSplitter(play):
    '''
    Function for splitting play string into the event, modifiers and advances fields
    
    Parameters
    -------------
    play : string containing the Retrosheet play
    
    Returns
    -------------
    dict : dictionary containing the following:
        'event' : string containing the primary event of the play
        'mods' : list of event modifiers
        'advs' : list of advances
    '''
    l = split_sep_rspct_paren(play)
    event = l[0]
    if re.match("\.",l[-1]):
        advs = re.split(";",l[-1].strip("."))
        mods = l[1:-1]
    else:
        advs = []
        mods = l[1:]
    if not isinstance(mods,list):
        mods = [mods]
    mods = [re.sub("^\/","",m) for m in mods]
    return({'event': event,
           'mods': mods,
           'advs': advs})
        
def _subeventParser(s):
    '''
    Function for producing details of individual components of
    the event play - i.e. subevents
    
    Parameters
    -------------
    s : string that represents the subevent 
    
    Returns
    -------------
    dict : dict contains:
         basicPlay: string with description of the subevent
         playersOut: list of players that are out on subevent
         implicitAdvances: list of implicit advances that may be
             missing from the play advance field
    '''
    playersOut = []
    implicitAdvances = []
    if re.match("[1-9]+(\([1-3B]\))?$",s) is not None:
        # no error, player is out
        pOut = [re.sub("\(|\)","",i) for i in re.findall("\([1-3B]\)",s)]
        if len(pOut) == 0:
            batterOut = True
            playersOut.append('B')
        else:
            if pOut[0] == 'B':
                batterOut = True
                playersOut.append('B')
            else:
                playersOut.append(pOut[0])
        p = 'Out'
    elif re.match("[1-9]+(\([1-3B]\))?.*E[1-9]",s) is not None:
        # there was an error - currently assuming no out
        p = 'Error'
    elif re.match('FC',s):
        p = "Fielder's Choice"
        implicitAdvances.append('B-1')
    elif re.match('C$',s):
        p = 'Interference'
        implicitAdvances.append('B-1')
    elif re.match('NP',s):
        p = 'No Play'
    elif re.match('K',s):
        p = 'Strikeout'
        playersOut.append('B')
    elif re.match('H[^P]',s):
        p = 'Hit - Home Run'
        implicitAdvances.append('B-H')
    elif re.match('HP',s):
        p = 'Hit by Pitch'
        implicitAdvances.append('B-1')
    elif re.match('S[1-9]*$',s):
        p = 'Hit - Single'
        implicitAdvances.append('B-1')
    elif re.match('D[1-9]*$',s):
        p = 'Hit - Double'
        implicitAdvances.append('B-2')
    elif re.match('DGR',s):
        p = 'Hit - Ground Rule Double'
        implicitAdvances.append('B-2')
    elif re.match('T[1-9]*$',s):
        p = 'Hit - Triple'
        implicitAdvances.append('B-3')
    elif re.match('CS[2-3H]',s) is not None:
        p = 'Runner Caught Stealing'
        for r in re.findall('CS[2-3H]',s):
            bbs = r.strip('CS')
            if bbs == 'H':
                br = '3'
            elif bbs == '3':
                br = '2'
            elif bbs == '2':
                br = '1'
            playersOut.append(br)
    elif re.match('WP',s):
        p = 'Wild Pitch'
    elif re.match('W$',s):
        p = 'Walk'
        implicitAdvances.append('B-1')
    elif re.match('IW',s):
        p = 'Intentional Walk'
        implicitAdvances.append('B-1')
    elif re.match('PB',s):
        p = 'Passed Ball'
    elif re.match('SB(2|3|H)?',s):
        p = 'Stolen Base'
    elif re.match('E[0-9]',s):
        p = 'Error'
    elif re.match('DI',s):
        p = 'Defensive Indifference'
    elif re.match('PO[1-3]',s):
        p = 'Picked Off Base'
        for r in re.findall('PO[1-3](\([1-9]*E[1-9])?',s):
            if re.search("E[1-9]",r) is not None:
                p = 'Picked Off Base - Error'
            else:
                br = re.sub('PO','',r)
                playersOut.append(br)
    elif re.match('POCS[2-3H]',s):
        p = 'Picked Off Base Caught Stealing'
        for r in re.findall('POCS[2-3H](\([1-9]*E[1-9])?',s):
            if re.search("E[1-9]",r) is not None:
                p = 'Picked Off Base Caught Stealing - Error'
            else:
                for r in re.findall('POCS[2-3H]',s):
                    bbs = r.strip('POCS')[0]
                    if bbs == 'H':
                        br = '3'
                    elif bbs == '3':
                        br = '2'
                    elif bbs == '2':
                        br = '1'
                    playersOut.append(br)
    elif re.match('BK',s):
        p = 'Balk'
    elif re.match('OA',s):
        p = 'Other Baserunner Advance'
    elif re.match('FLE',s):
        p = 'Error on Foul Flyball'
    else:
        p = ''

    return({'basicPlay' : p,
            'playersOut' : playersOut,
            'implicitAdvances': implicitAdvances})


def basicPlayDesc(subevents):
    '''
    Function for getting the basic play description from list of
    parsed subevents
    
    Parameters
    -------------
    subevents : list of dictionaries containing parsed subevents.
       Each dictionary must contain the key 'basicPlay'
    
    Returns
    -------------
    basicPlayDesc : string containing the description of the basic play
    '''
    if len(subevents) == 0:
        raise ValueError("Subevent length must be greater than 0")
    elif len(subevents) == 1:
        basicPlayDesc = subevents[0]['basicPlay']
    elif len(subevents) > 1:
        if subevents[0]['basicPlay'] == 'Out':
            nOuts = sum([int(i['basicPlay'] == 'Out') for i in subevents])
            if nOuts == 1:
                basicPlayDesc = 'Out'
            elif nOuts == 2:
                basicPlayDesc = 'Double Play'
            elif nOuts == 3:
                basicPlayDesc = 'Triple Play'
        else:
            basicPlayDesc = subevents[0]['basicPlay']
    return(basicPlayDesc)
            

def fullPlayDesc(subevents):
    '''
    Function for getting the full play description from list of
    parsed subevents
    
    Parameters
    -------------
    subevents : list of dictionaries containing parsed subevents.
       Each dictionary must contain the key 'basicPlay'
    
    Returns
    -------------
    fullPlayDesc : string containing the description of the full play
    '''
    if len(subevents) == 0:
        raise ValueError("Subevent length must be greater than 0")
    elif len(subevents) == 1:
        fullPlayDesc = subevents[0]['basicPlay']
    elif len(subevents) > 1:
        if subevents[0]['basicPlay'] == 'Out':
            nOuts = sum([int(i['basicPlay'] == 'Out') for i in subevents])
            if nOuts == 1:
                fullPlayDesc = 'Out'
            elif nOuts == 2:
                fullPlayDesc = 'Double Play'
            elif nOuts == 3:
                fullPlayDesc = 'Triple Play'
        else:
            fullPlayDesc = " - ".join([i['basicPlay'] for i in subevents])
    return(fullPlayDesc)


def playersOut(playSplit,subeventsParsed):
    '''
    Function for getting the list of players out on the play
    
    Parameters
    -------------
    playSplit : dictionary containing 'advs' - list of advances from the advances field
    subeventsParsed : list of dictionaries containing parsed subevents.
       Each dictionary must contain the key 'playersOut'
    
    Returns
    -------------
    playersOutList : list of players out on the play
    '''
    subeventsOuts = [j for i in subeventsParsed for j in i['playersOut']]
    # handle outs from advances:
    advNotOut = []
    advOuts = []
    advs = playSplit['advs']
    for i in advs:
        if re.match('[1-3B]X.*E',i):
            advNotOut.append(i[0])
        elif re.match('[1-3B]X',i):
            advOuts.append(i[0])
        elif re.match('[1-3B]-',i):
            advNotOut.append(i[0])
    playersOutList = list(
        set().union(advOuts,subeventsOuts)
             .difference(set(advNotOut)))
    return(playersOutList)


def playerAdvances(playSplit,subeventsParsed):
    '''
    Function for getting ALL player advances
    
    Parameters
    -------------
    playSplit : dictionary containing 'advs' - list of advances from the advances field
    subeventsParsed : list of dictionaries containing parsed subevents.
       Each dictionary must contain the key 'implicitAdvances'
    
    Returns
    -------------
    dict : dictionary of player advances where key is the initial base position
        and value is the final base position
    '''
    advances = dict()
    implicitAdvances = [j for i in subeventsParsed for j in i['implicitAdvances']]
    advs = playSplit['advs']
    advsNotOut = []
    for i in advs:
        if re.match('[1-3B]X[1-3H].*E',i):
            # error on the play, so player did advance
            advsNotOut.append(i[0:3])
        elif re.match('[1-3B]-[1-3H]',i):
            advsNotOut.append(i[0:3])
    ria = list(set([i for i in implicitAdvances for j in advs if i[0] == j[0]]))
    finalAdvs = list(set(advsNotOut + implicitAdvances) - set(ria))
    return({i[0]:i[2] for i in finalAdvs})

def runsScored(playerAdvances):
    runs = sum([i == 'H' for i in playerAdvances.values()])
    return(runs)

def enhancePlays(plays):
    '''
    Funtion for applying retrosheetParser functions to create
    additional attribution
    Parameters
    -------------
    plays : pandas DataFrame containing column 'Play'
    
    Returns
    -------------
    new_df : pandas DataFrame with additional attribution
    '''
    new_df = (plays
    .query("Type == 'play'")
    .assign(playSplit = lambda x: x.Play.transform(_playSplitter))
    .assign(subevents = lambda x: x.playSplit.apply(lambda z: _subevent_splitter(z['event'])))
    .assign(subeventParsed = lambda x: x.subevents.apply(lambda z: [_subeventParser(j) for j in z]))
    .assign(basicPlayDesc = lambda x: x.subeventParsed.transform(basicPlayDesc))
    .assign(fullPlayDesc = lambda x: x.subeventParsed.transform(fullPlayDesc))
    .assign(playersOut = lambda x: x.apply(lambda z: playersOut(z.playSplit,z.subeventParsed), axis = 1))
    .assign(outsOnPlay = lambda x: x.playersOut.apply(len))
    .assign(playerAdvances = lambda x: x.apply(lambda z: playerAdvances(z.playSplit,z.subeventParsed), axis = 1))
    .assign(runsScored = lambda x: x.playerAdvances.transform(runsScored))
    )
    return(new_df)

def combineGames(games):
    combinedGames = pd.concat([(games[i]['plays']
        .assign(Game = games[i]['id'])
        .set_index(['Game',games[i]['plays'].index])
        .rename_axis(['Game','PlayNum'])) for i in range(len(games))])
    return(combinedGames)

def RetrosheetToJson(lst):
    d = RetrosheetEventFormatter(lst)
    for k in d.keys():
        if isinstance(d[k],pd.DataFrame):
            d[k] = d[k].to_json(orient='split')
    d = json.dumps(d)
    return(d)

def RetrosheetToDict(lst):
    d = RetrosheetEventFormatter(lst)
    for k in d.keys():
        if isinstance(d[k],pd.DataFrame):
            d[k] = d[k].to_dict(orient='split')
    return(d)