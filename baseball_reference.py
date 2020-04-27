from requests import get
from contextlib import closing
from bs4 import BeautifulSoup as BS
from bs4 import SoupStrainer
from bs4 import Comment
import re
import pandas as pd

def get_boxes_urls(season_yr):
    '''
    Function to get urls from baseball-reference.com to pages
    that contain play-by-play data for games in MLB season 'season_yr'
    
    Parameters
    -------------
    season_yr : string or number representing season year
    
    Returns
    -------------
    links : list of relative urls for pages containing play-by-play data
    '''
    url_form = ('https://www.baseball-reference.com/leagues/MLB/' +
                str(season_yr) + '-schedule.shtml')
    with closing(get(url_form, stream=True)) as resp:
        x = resp.content
    links = []
    for link in BS(x, features="lxml", parse_only=SoupStrainer('a')):
        if hasattr(link, "href"):
            if re.match("\/boxes\/[A-Z]{3}\/",link['href']):
                links.append(link['href'])
    return(links)

def get_play_by_play(box_url):
    '''
    Function to get play-by-play data from baseball-reference.com
    for MLB game corresponding to the relative url 'box_url'
    
    Parameters
    -------------
    box_url : url relative to baseball-reference.com containing
              play-by-play data for specific MLB game
    
    Returns
    -------------
    play_by_play_tbls : list of pandas DataFrames containing play-by-play
                        data parsed from html table with id="play_by_play"
    '''
    url = 'https://www.baseball-reference.com' + box_url
    with closing(get(url, stream=True)) as resp:
        x = resp.content.decode('utf-8')
    soup = BS(x, 'html.parser', parse_only = SoupStrainer('div',{'id': 'all_play_by_play'}))
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    play_by_play_tbls = []
    for c in comments:
        if re.search(' id="play_by_play" ',c) is not None:
            z = pd.read_html(c.extract())
            play_by_play_tbls.append(z[0])
    return(play_by_play_tbls)