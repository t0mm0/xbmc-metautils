import xbmc, xbmcgui, xbmcaddon, xbmcplugin
import urllib, urllib2
import re, string
from t0mm0.common.addon import Addon
from t0mm0.common.net import Net
from metahandler import metahandlers, metacontainers

addon = Addon('plugin.video.tester', sys.argv)
xaddon = xbmcaddon.Addon(id='plugin.video.meta.tester')
net = Net()

mode = addon.queries['mode']
play = addon.queries.get('play', None)
name = addon.queries.get('name', None)
imdb_id = addon.queries.get('imdb_id', None)
tmdb_id = addon.queries.get('tmdb_id', None)
video_type = addon.queries.get('video_type', None)
season = addon.queries.get('season', None)

def add_contextmenu(watched_mark, title, type, imdb, tmdb='', year=''):
    contextmenuitems = []
    contextmenuitems.append((watched_mark, 'XBMC.RunPlugin(%s?mode=watch_mark&video_type=%s&name=%s&imdb_id=%s&tmdb_id=%s)' % (sys.argv[0], type, title, imdb, tmdb)))
    contextmenuitems.append(('Refresh Metadata', 'XBMC.RunPlugin(%s?mode=refresh_meta&name=%s&imdb_id=%s&tmdb_id=%s&year=%s)' % (sys.argv[0], title, imdb, tmdb, year)))
    return contextmenuitems


def add_video(meta, type):
    if meta['overlay'] == 6:
        watched_mark = 'Mark Watched'
    else:
        watched_mark = 'Mark Unwatched'
    
    if type == 'movie':
        contextMenuItems = add_contextmenu(watched_mark, meta['title'], type, meta['imdb_id'], meta['tmdb_id'], meta['year'])
        addon.add_video_item({'url': 'none', 'video_type': type}, meta, contextMenuItems, img=meta['cover_url'], fanart=meta['backdrop_url'])    
    if type == 'tvshow':
        contextMenuItems = add_contextmenu(watched_mark, meta['title'], type, imdb_id)    
        addon.add_video_item({'url': 'none', 'video_type': type}, meta, contextMenuItems, img=meta['cover_url'])    

    
if mode == 'main':
       
    metaget=metahandlers.MetaData()
    
    #Search by IMDB ID   
    meta = metaget.get_meta('movie','The Hangover',imdb_id='tt1119646')
    add_video(meta, 'movie')

    #Search by Name + Year
    meta = metaget.get_meta('movie','40 Year Old Virgin', year='2005')
    add_video(meta, 'movie')    

    #Search by TMDB ID
    meta = metaget.get_meta('movie','Horrible Bosses', tmdb_id='51540')  
    add_video(meta, 'movie')
            
    #Search for TV Show
    meta = metaget.get_meta('tvshow','The Simpsons')
    addon.add_directory({'url': 'none', 'mode': 'tvseasons', 'imdb_id': meta['imdb_id']}, meta, img=meta['cover_url'], fanart=meta['backdrop_url'])
    

    #episode=metaget.get_episode_meta('tt0096697', '1', '1')
    #print episode
   
    
elif mode == 'watch_mark':
    metaget=metahandlers.MetaData()
    metaget.change_watched(video_type, name, imdb_id, tmdb_id)
    xbmc.executebuiltin("Container.Refresh")


elif mode == 'refresh_meta':
    year = addon.queries.get('year', None)
    metaget=metahandlers.MetaData()
    search_meta = metaget.search_movies(name)
    
    if search_meta:
        movie_list = []
        for movie in search_meta:
            movie_list.append(movie['title'] + ' (' + str(movie['year']) + ')')
        dialog = xbmcgui.Dialog()
        index = dialog.select('Choose', movie_list)
        
        if index > -1:
            new_imdb_id = search_meta[index]['imdb_id']
            new_tmdb_id = search_meta[index]['tmdb_id']       
            meta = metaget.update_meta(name, old_imdb_id=imdb_id, old_tmdb_id=tmdb_id, new_imdb_id=new_imdb_id, new_tmdb_id=new_tmdb_id, year=year)   
            xbmc.executebuiltin("Container.Refresh")
    else:
        msg = ['No matches found']
        addon.show_ok_dialog(msg, 'Refresh Results')


elif mode == 'tvseasons':   
    metaget=metahandlers.MetaData()
    season_list = ['1','2', '3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23']
    season_meta = metaget.get_seasons(imdb_id, season_list)
    for season in season_list:
        cur_season = season_meta[int(season) - 1]
        addon.add_directory({'mode': 'tvepisodes', 'url': 'none', 'imdb_id': imdb_id, 'season': season}, {'title': 'Season ' + season}, total_items=len(season_list), img=cur_season['cover_url'])
        

elif mode == 'tvepisodes':   
    metaget=metahandlers.MetaData()
    episodes = range(1,10)
    for episode in episodes:
        episode_meta=metaget.get_episode_meta(imdb_id, season, episode)
        add_video(episode_meta, 'episode')


if not play:
    addon.end_of_directory()