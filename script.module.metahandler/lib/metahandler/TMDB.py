# Credits: Daledude, WestCoast13
# Awesome efficient lightweight code.
# last modified 19 March 2011
# added support for TVDB search for show, seasons, episodes
# also searches imdb (using http://www.imdbapi.com/) for missing info in movies or tvshows

import simplejson
import urllib, re, socket
from t0mm0.common.net import Net         
net = Net()

class TMDB(object):
    def __init__(self, api_key='b91e899ce561dd19695340c3b26e0a02', view='json', lang='en'):
        #view = yaml json xml
        self.view = view
        self.lang = lang
        self.api_key = api_key
        self.url_prefix = 'http://api.themoviedb.org/2.1'
        self.imdb_api = 'http://www.imdbapi.com/?i=%s'
        self.imdb_name_api = 'http://www.imdbapi.com/?t=%s'
        self.imdb_nameyear_api = 'http://www.imdbapi.com/?t=%s&y=%s' 

            
    def _do_request(self, method, values):
        url = "%s/%s/%s/%s/%s/%s" % (self.url_prefix, method, self.lang, self.view, self.api_key, values)
        print 'Requesting TMDB : %s' % url
        try:
            meta = simplejson.loads(net.http_GET(url).content)[0]
        except Exception, e:
            print "Error connecting to TMDB: %s " % e
            return None

        if meta == 'Nothing found.':
            return None
        else:
            return meta


    def _upd_key(self, meta, key):
        if meta.has_key(key) == False :
            return True 
        else:
            try:
                if key == '' or key == '0.0' or key == '0' or key == 'None' or key == '[]' or key == 'No overview found.' or key == 'TBD':
                    return True
                else:
                    return False
            except:
                return True


    def search_imdb(self, name, imdb_id='', year=''):
        
        #Set IMDB API URL based on the type of search we need to do
        if imdb_id:
            url = self.imdb_api % imdb_id
        else:
            name = urllib.quote(name)
            if year:
                url = self.imdb_nameyear_api % (name, year)
            else:
                url = self.imdb_name_api % name

        try:
            print 'Requesting IMDB : %s' % url
            meta = simplejson.loads(net.http_GET(url).content)
        except Exception, e:
            print "Error connecting to IMDB: %s " % e
            return {}

        if not meta['Response'] == 'True':
            return {}
        else:
            return meta
        

    def update_imdb_meta(self,meta, imdb_meta):
    
        print 'Updating TMDB meta with IMDB'        
        if self._upd_key(meta, 'overview'):
            if imdb_meta.has_key('Plot'):
                meta['overview']=imdb_meta['Plot']           

        if self._upd_key(meta, 'posters') and self._upd_key(meta, 'imdb_poster'):
            temp=imdb_meta['Poster']
            if temp != 'N/A':
                meta['imdb_poster']=temp
        if self._upd_key(meta, 'rating'):
            temp=imdb_meta['Rating']
            if temp != 'N/A' and temp !='' and temp != None:
                meta['rating']=temp
        if self._upd_key(meta, 'genre') and self._upd_key(meta, 'imdb_genres'):
            temp=imdb_meta['Genre']
            if temp != 'N/A':
                meta['imdb_genres']=temp
        if self._upd_key(meta, 'runtime'):
            temp=imdb_meta['Runtime']
            if temp != 'N/A':
                dur=0
                scrape=re.compile('(.+?) hr').findall(temp)
                if len(scrape) > 0:
                    dur = int(scrape[0]) * 60
                scrape=re.compile(' (.+?) (.+?) min').findall(temp)
                if len(scrape) > 0:
                    dur = dur + int(scrape[0][1])
                else: # No hrs in duration
                    scrape=re.compile('(.+?) min').findall(temp)
                    if len(scrape) > 0:
                        dur = dur + int(scrape[0])
                meta['runtime']=str(dur)
        
        meta['code'] = imdb_meta['ID']
        return meta


    # video_id is either tmdb or imdb id
    def getVersion(self, video_id):
        return self._do_request('Movie.getVersion', video_id)


    def getInfo(self, tmdb_id):
        return self._do_request('Movie.getInfo', tmdb_id)
        

    def searchMovie(self, name, year=''):
        if year:
            name = urllib.quote(name) + '+' + year
        return self._do_request('Movie.search',name)
        

    def getSeasonPosters(self, tvdb_id, season):
        tvdb = TheTVDB()
        images = tvdb.get_show_image_choices(tvdb_id)
        
        return images


    def tmdb_lookup(self, name, imdb_id='', year=''):
        tmdb_id = ''
        meta = {}
        
        #If we don't have an IMDB ID let's try searching TMDB first by movie name
        if not imdb_id:
            meta = self.searchMovie(name,year)              
            if meta:
                tmdb_id = meta['id']
                imdb_id = meta['imdb_id']
            
            #Didn't get a match by name at TMDB, let's try IMDB by name
            else:
                meta = self.search_imdb(name, imdb_id, year)
                if meta:
                    imdb_id = meta['ID']       

        #If we don't have a tmdb_id yet but do have imdb_id lets see if we can find it
        if not tmdb_id and imdb_id:
            meta = self.getVersion(imdb_id)
            if meta:
                tmdb_id = meta['id']

        if tmdb_id:
            meta = self._do_request('Movie.getInfo', tmdb_id)

            if meta is None: # fall through to IMDB lookup
                meta = {}
            else:               
                
                if meta['overview'] == 'None' or meta['overview'] == '' or meta['overview'] == 'TBD' or meta['overview'] == 'No overview found.' or meta['rating'] == 0 or meta['runtime'] == 0 or str(meta['genres']) == '[]' or str(meta['posters']) == '[]':
                    print 'Some info missing in TMDB for Movie *** %s ***. Will search imdb for more' % imdb_id
                    imdb_meta = self.search_imdb(name, imdb_id)
                    meta = self.update_imdb_meta(meta, imdb_meta)
        
        #If all else fails, and we don't have a TMDB id
        else:
            meta = {}
            imdb_meta = self.search_imdb(name, imdb_id, year)
            if imdb_meta:
                meta = self.update_imdb_meta({}, imdb_meta)
       
        meta['code'] = imdb_id
        return meta


    def check(self, value, ret=None):
        if value is None or value == '':
            if ret == None:
                return ''
            else:
                return ret
        else:
            return value


    def get_date(self, year, month_day):
        month_name = month_day[:3]
        day=month_day[4:]
        
        if month_name=='Jan':
            month='01'
        elif month_name=='Feb':
            month='02'
        elif month_name=='Mar':
            month='03'
        elif month_name=='Apr':
            month='04'
        elif month_name=='May':
            month='05'
        elif month_name=='Jun':
            month='06'
        elif month_name=='Jul':
            month='07'
        elif month_name=='Aug':
            month='08'
        elif month_name=='Sep':
            month='09'
        elif month_name=='Oct':
            month='10'
        elif month_name=='Nov':
            month='11'
        elif month_name=='Dec':
            month='12'
        
        print year + '-' + month + '-' + day
        
        return year + '-' + month + '-' + day
        

    def tvdbLookup(self, tvdb_id, season_num, episode_num, dateSearch):
        #TvDB Lookup for episodes
        
        meta = {}
        tvdb = TheTVDB()
        if dateSearch:
            aired=self.get_date(season_num, episode_num)
            episode = tvdb.get_episode_by_airdate(tvdb_id, aired)
            
            #We do this because the airdate method returns just a part of the overview unfortunately
            if episode is not None:
                ep_id = episode.id
                if ep_id is not None:
                    episode = tvdb.get_episode(ep_id)
        else:
            episode = tvdb.get_episode_by_season_ep(tvdb_id, season_num, episode_num)
            
        if episode is None:
            return None
        
        meta['episode_id'] = episode.id
        meta['tvdb_name'] = self.check(episode.name)
        meta['plot'] = self.check(episode.overview)
        '''if episode.season_number is not None and episode.episode_number is not None:
            meta['plot'] = "Episode: " + episode.season_number + "x" + episode.episode_number + "\n" + meta['plot']
        if episode.first_aired is not None:
            meta['plot'] = "Aired  : " + episode.first_aired + "\n" + meta['plot']
        if episode.name is not None:
            meta['plot'] = episode.name + "\n" + meta['plot']'''
        meta['rating'] = self.check(episode.rating,0)
        meta['aired'] = self.check(episode.first_aired)
        meta['poster'] = self.check(episode.image)
        meta['season_num'] = self.check(episode.season_number,0)
        meta['episode_num'] = self.check(episode.episode_number,0)
        
        '''
        show_and_episodes = tvdb.get_show_and_episodes(tvdb_id)
        if show_and_episodes == None:
            return meta
        
        (show, ep_list) = show_and_episodes
        for episode in ep_list:
            print '      S'+ episode.season_number + '.E' + episode.episode_number
            if episode.season_number == season_num and episode.episode_number == episode_num:
                meta['']=''
                break
        '''
        
        return meta