# Credits: Daledude, WestCoast13
# Awesome efficient lightweight code.
# last modified 19 March 2011
# added support for TVDB search for show, seasons, episodes
# also searches imdb (using http://www.imdbapi.com/) for missing info in movies or tvshows

import simplejson
import urllib, re
from datetime import datetime
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
            print 'TMDB Meta: ', meta            
            return meta


    def _convert_date(self, string):
        ''' Quick helper method to convert a string date in format dd MMM YYYY to YYYY-MM-DD '''
        try:
            d = datetime.strptime(string, '%d %b %Y')
            e = d.strftime('%Y-%m-%d')
        except Exception, e:
            print 'Date conversion failed: %s' % e
            return None
        return e
        
        
    def _upd_key(self, meta, key):
        if meta.has_key(key) == False :
            return True 
        else:
            try:
                bad_list = ['', '0.0', '0', 'None', '[]', 'No overview found.', 'TBD', None]
                if meta[key] in bad_list:
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
            print 'IMDB Meta: ', meta
        except Exception, e:
            print "Error connecting to IMDB: %s " % e
            return {}

        if meta['Response'] == 'True':
            return meta
        else:
            return {}
        

    def update_imdb_meta(self, meta, imdb_meta):
    
        print 'Updating current meta with IMDB'
        
        if self._upd_key(meta, 'overview'):
            print '-- IMDB - Updating Overview'
            if imdb_meta.has_key('Plot'):
                meta['overview']=imdb_meta['Plot']           
        
        if self._upd_key(meta, 'released'):
            print '-- IMDB - Updating Premiered'
            temp=imdb_meta['Released']
            if temp != 'N/A':
                meta['released'] = self._convert_date(temp)
            else:
                if imdb_meta['Year'] != 'N/A':
                    meta['released'] = imdb_meta['Year'] + '-01-01'
        
        if self._upd_key(meta, 'posters'):
            print '-- IMDB - Updating Posters'
            temp=imdb_meta['Poster']
            if temp != 'N/A':
                meta['cover_url']=temp
                
        if self._upd_key(meta, 'rating'):
            print '-- IMDB - Updating Rating'
            temp=imdb_meta['Rating']
            if temp != 'N/A' and temp !='' and temp != None:
                meta['rating']=temp
                
        if self._upd_key(meta, 'genre'):
            print '-- IMDB - Updating Genre'
            temp=imdb_meta['Genre']
            if temp != 'N/A':
                meta['genre']=temp
                
        if self._upd_key(meta, 'runtime'):
            print '-- IMDB - Updating Runtime'
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
    def _get_version(self, video_id):
        return self._do_request('Movie.getVersion', video_id)


    def _get_info(self, tmdb_id):
        return self._do_request('Movie.getInfo', tmdb_id)
        

    def _search_movie(self, name, year=''):
        if year:
            name = urllib.quote(name) + '+' + year
        return self._do_request('Movie.search',name)
        

    def tmdb_lookup(self, name, imdb_id='', year=''):
        tmdb_id = ''
        meta = {}
        
        #If we don't have an IMDB ID let's try searching TMDB first by movie name
        if not imdb_id:
            meta = self._search_movie(name,year)              
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
            print 'IMDB ID found, attempting to get TMDB ID'
            meta = self._get_version(imdb_id)
            if meta:
                tmdb_id = meta['id']

        if tmdb_id:
            meta = self._get_info(tmdb_id)

            if meta is None: # fall through to IMDB lookup
                meta = {}
            else:               
                
                if meta['overview'] == 'None' or meta['overview'] == '' or meta['overview'] == 'TBD' or meta['overview'] == 'No overview found.' or meta['rating'] == 0 or meta['runtime'] == 0 or str(meta['genres']) == '[]' or str(meta['posters']) == '[]' or meta['released'] == None:
                    print 'Some info missing in TMDB for Movie *** %s ***. Will search imdb for more' % imdb_id
                    imdb_meta = self.search_imdb(name, imdb_id)
                    if imdb_meta:
                        meta = self.update_imdb_meta(meta, imdb_meta)
        
        #If all else fails, and we don't have a TMDB id
        else:
            meta = {}
            imdb_meta = self.search_imdb(name, imdb_id, year)
            if imdb_meta:
                meta = self.update_imdb_meta({}, imdb_meta)
       
        meta['code'] = imdb_id
        return meta
