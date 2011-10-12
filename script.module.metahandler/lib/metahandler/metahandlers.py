'''
    These classes cache non-existant metadata from TheMovieDB and TVDB.
    It uses sqlite databases.
       
    It uses themoviedb JSON api class and TVDB XML api class.
    For TVDB it currently uses a modified version of 
    Python API by James Smith (http://loopj.com)
    
    Metahandler intially created for IceFilms addon, reworked to be it's own 
    script module to be used by many addons.

    Created/Modified by: Eldorado
    
    Initial creation and credits: Daledude / Anarchintosh / WestCoast13 
    
    
*To-Do:
- write a clean database function (correct imgs_prepacked by checking if the images actually exist)
  for pre-packed container creator. also retry any downloads that failed.
  also, if  database has just been created for pre-packed container, purge all images are not referenced in database.

'''

import os
import re
import sys
import base64
import xbmc, xbmcaddon
from TMDB import TMDB
from thetvdbapi import TheTVDB

''' Use t0mm0's common library for http calls, corrects unicode problems '''
from t0mm0.common.net import Net
net = Net()


'''
   Use SQLIte3 wherever possible, needed for newer versions of XBMC
   Keep pysqlite2 for legacy support
'''
try: 
    from sqlite3 import dbapi2 as sqlite
except: 
    from pysqlite2 import dbapi2 as sqlite

addon = xbmcaddon.Addon(id='script.module.metahandler')
path = addon.getAddonInfo('path')
sys.path.append((os.path.split(path))[0])


def make_dir(mypath, dirname):
    ''' Creates sub-directories if they are not found. '''
    subpath = os.path.join(mypath, dirname)
    if not os.path.exists(subpath): os.makedirs(subpath)
    return subpath


def bool2string(myinput):
    ''' Neatens up usage of preparezip flag. '''
    if myinput is False: return 'false'
    elif myinput is True: return 'true'


class MetaData:  
    '''
    This class performs all the handling of meta data, requesting, storing and sending back to calling application
    
        - Create cache DB if it does not exist
        - Create a meta data zip file container to share
        - Get the meta data from TMDB/IMDB/TVDB
        - Store/Retrieve meta from cache DB
        - Download image files locally
    '''  
    
     
    def __init__(self, path='special://profile/addon_data/script.module.metahandler/', preparezip=False):

        self.path = xbmc.translatePath(path)
        self.cache_path = make_dir(self.path, 'meta_cache')

        if preparezip:
            #create container working directory
            #!!!!!Must be matched to workdir in metacontainers.py create_container()
            self.work_path = make_dir(self.path, 'work')
            
        #this init auto-constructs necessary folder hierarchies.

        # control whether class is being used to prepare pre-packaged .zip
        self.classmode = bool2string(preparezip)
        self.videocache = os.path.join(self.cache_path, 'video_cache.db')

        self.tvpath = make_dir(self.cache_path, 'tvshow')
        self.tvcovers = make_dir(self.tvpath, 'covers')
        self.tvbackdrops = make_dir(self.tvpath, 'backdrops')

        self.mvpath = make_dir(self.cache_path, 'movie')
        self.mvcovers = make_dir(self.mvpath, 'covers')
        self.mvbackdrops = make_dir(self.mvpath, 'backdrops')

        # connect to db at class init and use it globally
        self.dbcon = sqlite.connect(self.videocache)
        self.dbcon.row_factory = sqlite.Row # return results indexed by field names and not numbers so we can convert to dict
        self.dbcur = self.dbcon.cursor()

        # create cache db if it doesn't exist
        self._cache_create_movie_db()


    def __del__(self):
        ''' Cleanup db when object destroyed '''
        self.dbcur.close()
        self.dbcon.close()

    def _string_compare(self, s1, s2):
        """ Method that takes two strings and returns True or False, based
            on if they are equal, regardless of case.
        """
        try:
            return s1.lower() == s2.lower()
        except AttributeError:
            print "Please only pass strings into this method."
            print "You passed a %s and %s" % (s1.__class__, s2.__class__)

   
    def _clean_string(self, string):
        """ 
            Method that takes a string and returns it cleaned of any special characters
            in order to do proper string comparisons
        """        
        try:
            return ''.join(e for e in string if e.isalnum())
        except:
            return string
    
    def _downloadimages(self,meta,mediatype,name):
        '''
        Download images to save locally
        
        Args:
            meta (dict): meta data dict
            mediatype (str): 'movies' or 'tvshow'
            name (str): filename to download
        '''                 
        if mediatype=='movies':
             cover_folder=os.path.join(self.mvcovers,name)

             if not os.path.exists(cover_folder):
                 os.makedirs(cover_folder)

             cover_name=self._picname(meta['cover_url'])
             cover_path = os.path.join(cover_folder, cover_name)

             self._dl_code(meta['cover_url'],cover_path)
             
             #backdrop_name=self._picname(meta['backdrop_url'])
             #backdrop_path = os.path.join(self.mvbackdrops, backdrop_name)
           
             #self._dl_code(meta['backdrop_url'],backdrop_path)
             
        if mediatype=='tvshow':
             outpath = os.path.join(self.tvimgpath, 'hi')
        if mediatype=='episode':
             pass
   

    def _picname(self,url):
        '''
        Get image name from url (ie my_movie_poster.jpg)      
        
        Args:
            url (str): full url of image                        
        Returns:
            picname (str) representing image name from file
        '''           
        picname = re.split('\/+', url)
        return picname[-1]
         
        
    def _dl_code(self,url,mypath):
        '''
        Downloads images to store locally       
        
        Args:
            url (str): url of image to download
            mypath (str): local path to save image to                       
        '''        
        print 'Attempting to download image from url: %s ' % url
        print 'Saving to destination: %s ' % mypath
        if url.startswith('http://'):
          
            try:
                 data = net.http_GET(url).content
                 fh = open(mypath, 'wb')
                 fh.write(data)  
                 fh.close()
            except Exception, e:
                print 'Image download failed: %s ' % e
        else:
            if url is not None:
                print 'Not a valid url: %s ' % url
      

    def get_meta(self, imdb_id, type, name, year=''):
        '''
        Main method to get meta data for movie or tvshow. Will lookup by name/year 
        if no IMDB ID supplied.       
        
        Args:
            imdb_id (str): IMDB ID
            type (str): 'movie' or 'tvshow'
            name (str): full name of movie/tvshow you are searching            
        Kwargs:
            year (str): 4 digit year of video, recommended to include the year whenever possible
                        to maximize correct search results.
                        
        Returns:
            DICT of meta data or None if cannot be found.
        '''
        
        print 'Attempting to retreive meta data for %s: %s %s %s' % (type, name, year, imdb_id)
        
        if imdb_id:
            # add the tt if not found. integer aware.
            imdb_id=str(imdb_id)
            if not imdb_id.startswith('tt'):
                imdb_id = "tt%s" % imdb_id

            meta = self._cache_lookup_by_imdb(imdb_id, type)
        else:
            meta = self._cache_lookup_by_name(type, name, year)

        if not meta:
            
            if type=='movie':
                meta = self._get_tmdb_meta(imdb_id, name, year)
            elif type=='tvshow':
                meta = self._get_tvdb_meta(imdb_id, name, year)
            
            meta = self._format_meta(meta, imdb_id, name, year)
            
            meta['overlay'] = self.get_watched( imdb_id, 'movie')
            self._cache_save_video_meta(meta, type)

            #if creating a metadata container, download the images.
            if self.classmode == 'true':
                self._downloadimages(meta,'movies',imdb_id)

        if meta:

            #Change cast back into a tuple
            if meta['cast']:
                meta['cast'] = eval(meta['cast'])
            
            #if cache row says there are pre-packed images...
            if meta['imgs_prepacked'] == 'true':
                    encoded_name = base64.b64encode(meta['title'])

                    #define the image paths
                    cover_path = os.path.join(self.mvcovers, imdb_id, self._picname(meta['cover_url']))
                    #backdrop_path=os.path.join(self.mvbackdrops,imdb_id,self._picname(meta['backdrop_url']))

                    #if paths exist, replace the urls with paths
                    if self.classmode == 'false':
                        if os.path.exists(cover_path):
                            meta['cover_url'] = cover_path
                        #if os.path.exists(backdrop_path):
                        #    meta['backdrop_url'] = backdrop_path

                    #try some image redownloads if building container
                    elif self.classmode == 'true':
                        if not os.path.exists(cover_path):
                                self._downloadimages(meta,'movies',imdb_id)

                        #if not os.path.exists(backdrop_path):
                        #        self._downloadimages(meta,'movies',imdb_id)
           
        return meta

    
    def _cache_create_movie_db(self):
        ''' Creates the cache database and tables.  '''   
        
        print 'Cache database does not exist, creating...'
                 
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS movie_meta ("
                           "imdb_id TEXT, tmdb_id TEXT, title TEXT,"
                           "director TEXT, writer TEXT, tagline TEXT, cast TEXT,"
                           "rating FLOAT, duration TEXT, plot TEXT,"
                           "mpaa TEXT, premiered TEXT, genre TEXT, studio TEXT,"
                           "thumb_url TEXT, cover_url TEXT,"
                           "trailer_url TEXT, backdrop_url TEXT,"
                           "imgs_prepacked TEXT," # 'true' or 'false'. added to determine whether to load imgs from path not url (ie. if they are included in pre-packaged metadata container).
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, tmdb_id, title)"
                           ");"
        )
        self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on movie_meta (title);')
        print 'Table movie_meta created'
        
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS tvshow_meta ("
                           "imdb_id TEXT, tmdb_id TEXT, title TEXT,"
                           "director TEXT, writer TEXT, cast TEXT,"
                           "rating FLOAT, duration TEXT, plot TEXT,"
                           "mpaa TEXT, premiered TEXT, genre TEXT, studio TEXT,"
                           "thumb_url TEXT, cover_url TEXT,"
                           "trailer_url TEXT, backdrop_url TEXT,"
                           "imgs_prepacked TEXT," # 'true' or 'false'. added to determine whether to load imgs from path not url (ie. if they are included in pre-packaged metadata container).
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, tmdb_id, title)"
                           ");"
        )
        self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on tvshow_meta (title);')
        print 'Table tvshow_meta created'
        
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS episode_meta ("
                           "imdb_id TEXT, "
                           "tvdb_id TEXT, "
                           "season TEXT, "
                           "season_num INTEGER, "
                           "episode TEXT, "
                           "episode_num INTEGER, "
                           "episode_id TEXT, "
                           "title TEXT, "
                           "tvdb_name TEXT, "
                           "plot TEXT, "
                           "rating FLOAT, "
                           "aired TEXT, "
                           "poster TEXT, "
                           "overlay INTEGER, "
                           "UNIQUE(imdb_id, tvdb_id, season, title)"
                           ");"
        )
        print 'Table episode_meta created'

        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS season_meta ("
                           "imdb_id TEXT, tmdb_id TEXT, season TEXT,"
                           "cover_url TEXT,"
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, season)"
                           ");"
        )
               
        #self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on tvshow_meta (name);')
        print 'Table season_meta created'

    def _cache_lookup_by_imdb(self, imdb_id, type):
        '''
        Lookup in SQL DB for video meta data by IMDB ID
        
        Args:
            imdb_id (str): IMDB ID
            type (str): 'movie' or 'tvshow'
                        
        Returns:
            DICT of matched meta data or None if no match.
        '''        
        if type == 'movie':
            table='movie_meta'
        elif type == 'tvshow':
            table='tvshow_meta'
        # select * is easier since we return a dict but may not be efficient.
        self.dbcur.execute("SELECT * FROM " + table + " WHERE imdb_id = '%s'" % imdb_id) #select database row where imdb_id matches
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            print 'Found meta information by imdb id in cache table: ', dict(matchedrow)
            return dict(matchedrow)
        else:
            return None
    

    def _cache_lookup_by_name(self, type, name, year=''):
        '''
        Lookup in SQL DB for video meta data by name and year
        
        Args:
            type (str): 'movie' or 'tvshow'
            name (str): full name of movie/tvshow you are searching
        Kwargs:
            year (str): 4 digit year of video, recommended to include the year whenever possible
                        to maximize correct search results.
                        
        Returns:
            DICT of matched meta data or None if no match.
        '''        
        if type == 'movie':
            table='movie_meta'
        elif type == 'tvshow':
            table='tvshow_meta'
        name = name.replace("'","''")
        sql_select = "SELECT * FROM " + table + " WHERE title = '%s'" % name       
        if year:
            sql_select = sql_select + " AND strftime('%s',premiered) = '%s'" % ('%Y', year)
        self.dbcur.execute(sql_select)            
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            print 'Found meta information by name in cache table: ', dict(matchedrow)
            return dict(matchedrow)
        else:
            return None
                       

    def _cache_save_video_meta(self, meta, type):
        '''
        Saves meta data to SQL table given type
        
        Args:
            meta (dict): meta data of video to be added to database
            type (str): 'movie' or 'tvshow'
                        
        '''            
        if type == 'movie':
            table='movie_meta'
        elif type == 'tvshow':
            table='tvshow_meta'
        
        #Select on either IMDB ID or name + premiered
        if meta['code']:
            sql_select = "SELECT * FROM " + table + " WHERE imdb_id = '%s'" % meta['code']
        else:           
            sql_select = "SELECT * FROM " + table + " WHERE title = '%s' AND premiered = '%s'" % (meta['title'], meta['premiered'])
            
        try:
            self.dbcur.execute(sql_select) #select database row
            matchedrow = self.dbcur.fetchone()
            if matchedrow:
                    print 'Matched Row found, deleting table entry', matchedrow
                    self.dbcur.execute("DELETE FROM " + table + " WHERE imdb_id = '%s'" % meta['code']) #delete database row where imdb_id matches
        except Exception, e:
            print 'Error attempting to delete from cache table: %s ' % e
            print 'Meta data:', meta               
            pass
        
        meta['imdb_id'] = meta['code']
        meta['cast'] = str(meta['cast'])
        
        print 'Saving cache information: ', meta         
        try:
            if type == 'movie':
                self.dbcur.execute("INSERT INTO " + table + " VALUES "
                                   "(:imdb_id, :tmdb_id, :title, :director, :writer, :tagline, :cast, :rating, :duration, :plot, :mpaa, :premiered, :genre, :studio, :thumb_url, :cover_url, :trailer_url, :backdrop_url, :imgs_prepacked, :overlay)",
                                   meta
                                   #"('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                                   #% ( meta['code'], meta['tmdb_id'],meta['title'],meta['rating'],meta['duration'],meta['plot'],meta['mpaa'],
                                   #meta['premiered'],meta['genre'],meta['studio'],meta['thumb_url'],meta['cover_url'],meta['trailer_url'],meta['backdrop_url'],meta['imgs_prepacked'], meta['watched'])
                )
            elif type == 'tvshow':
                self.dbcur.execute("INSERT INTO " + table + " VALUES "
                                   "(:imdb_id, :tmdb_id, :title, :director, :writer, :cast, :rating, :duration, :plot, :mpaa, :premiered, :genre, :studio, :thumb_url, :cover_url, :trailer_url, :backdrop_url, :imgs_prepacked, :overlay)",
                                   meta
                                   #"('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                                   #% ( meta['code'], meta['tmdb_id'],meta['title'],meta['rating'],meta['duration'],meta['plot'],meta['mpaa'],
                                   #meta['premiered'],meta['genre'],meta['studio'],meta['thumb_url'],meta['cover_url'],meta['trailer_url'],meta['backdrop_url'],meta['imgs_prepacked'], meta['watched'])
                )
            self.dbcon.commit()
        except Exception, e:
            print 'Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass            
    

    def _get_tmdb_meta(self, imdb_id, name, year=''):
        '''
        Requests meta data from TMDB and creates proper dict to send back
        
        Args:
            imdb_id (str): IMDB ID
            name (str): full name of movie you are searching
        Kwargs:
            year (str): 4 digit year of movie, when imdb_id is not available it is recommended
                        to include the year whenever possible to maximize correct search results.
                        
        Returns:
            DICT. It must also return an empty dict when
            no movie meta info was found from tmdb because we should cache
            these "None found" entries otherwise we hit tmdb alot.
        '''        
        
        tmdb = TMDB()        
        meta = tmdb.tmdb_lookup(name,imdb_id,year)       
        
        if meta is None:
            # create an empty dict so below will at least populate empty data for the db insert.
            meta = {}
        else:
            if not imdb_id:
                imdb_id = meta['code']
 
        return meta

    
    def _get_tvdb_meta(self, imdb_id, name, year=''):
        '''
        Requests meta data from TVDB and creates proper dict to send back
        
        Args:
            imdb_id (str): IMDB ID
            name (str): full name of movie you are searching
        Kwargs:
            year (str): 4 digit year of movie, when imdb_id is not available it is recommended
                        to include the year whenever possible to maximize correct search results.
                        
        Returns:
            DICT. It must also return an empty dict when
            no movie meta info was found from tvdb because we should cache
            these "None found" entries otherwise we hit tvdb alot.
        '''      
        print 'Starting TVDB Lookup'
        tvdb = TheTVDB()
        tvdb_id = ''
        meta = {}
        if imdb_id:
            tvdb_id = tvdb.get_show_by_imdb(imdb_id)

        # if not found by imdb, try by name
        if tvdb_id == '':
            show_list=tvdb.get_matching_shows(name)
            print 'Found TV Show List: ', show_list
            tvdb_id=''
            prob_id=''
            for show in show_list:
                (junk1, junk2, junk3) = show
                #if we match imdb_id or full name (with year) then we know for sure it is the right show
                if junk3==imdb_id or self._string_compare(self._clean_string(junk2),self._clean_string(name)):
                    tvdb_id=self._clean_string(junk1)
                    imdb_id=self._clean_string(junk3)
                    break
                #if we match just the cleaned name (without year) keep the tvdb_id
                elif self._string_compare(self._clean_string(junk2),self._clean_string(name)):
                    prob_id = junk1
                    imdb_id = self_clean_string(junk3)
            if tvdb_id == '' and prob_id != '':
                tvdb_id = self._clean_string(prob_id)

        if tvdb_id:
            print 'Show *** ' + name + ' *** found in TVdb. Getting details...'
            show = tvdb.get_show(tvdb_id)
            if show is not None:
                meta['code'] = imdb_id
                meta['id'] = tvdb_id
                meta['name'] = name
                if str(show.rating) == '' or show.rating == None:
                    meta['rating'] = 0
                else:
                    meta['rating'] = show.rating
                meta['runtime'] = 0
                meta['overview'] = show.overview
                meta['certification'] = show.content_rating
                meta['released'] = show.first_aired
                meta['trailer_url'] = ''
                if show.genre != '':
                    temp = show.genre.replace("|",",")
                    temp = temp[1:(len(temp)-1)]
                    meta['genre'] = temp
                meta['tvdb_studios'] = show.network
                if show.actors != None:
                    num=1
                    meta['actors']=''
                    print show.actors
                    for actor in show.actors:
                        if num == 1:
                            meta['actors'] = actor
                        else:
                            meta['actors'] = meta['actors'] + ", " + actor
                            if num == 5: # Read only first 5 actors, there might be a lot of them
                                break
                        num = num + 1
                #meta['imgs_prepacked'] = self.classmode
                meta['imdb_poster'] = show.poster_url
                print 'cover is  *** ' + meta['imdb_poster']
                
                print '          rating ***' + str(meta['rating'])+'!!!'

                if meta['overview'] == 'None' or meta['overview'] == '' or meta['overview'] == 'TBD' or meta['overview'] == 'No overview found.' or meta['rating'] == 0 or meta['runtime'] == 0 or meta['actors'] == '' or meta['imdb_poster'] == '':
                    print ' Some info missing in TVdb for TVshow *** '+ name + ' ***. Will search imdb for more'
                    tmdb = TMDB()
                    imdb_meta = tmdb.search_imdb(name, imdb_id)
                    if imdb_meta:
                        meta = tmdb.update_imdb_meta(meta, imdb_meta)
                else:
                    meta['overview'] = 'Starring : \n' + meta['actors'] + '\n\nPlot : \n' + meta['overview']
                return meta
            else:
                tmdb = TMDB()
                imdb_meta = tmdb.search_imdb(name, imdb_id)
                if imdb_meta:
                    meta = tmdb.update_imdb_meta(meta, imdb_meta)
                return meta    
        else:
            return meta


    def _format_meta(self, md, imdb_id, name, year):
        '''
        Copy tmdb to our own for conformity and eliminate KeyError. Set default for values not returned
        
        Args:
            imdb_id (str): IMDB ID
            name (str): full name of movie you are searching
        Kwargs:
            year (str): 4 digit year of movie, when imdb_id is not available it is recommended
                        to include the year whenever possible to maximize correct search results.
                        
        Returns:
            DICT. It must also return an empty dict when
            no movie meta info was found from tvdb because we should cache
            these "None found" entries otherwise we hit tvdb alot.
        '''      
        
        #Ensure year has a set value
        if year:
            year = year + '-01-01'
        else:
            year = ''
            
        meta = {}
        meta['overlay'] = 6
        meta['code'] = md.get('code', imdb_id)
        meta['tmdb_id'] = md.get('id', '')
        meta['title'] = md.get('name', name)
        meta['writer'] = ''
        meta['director'] = ''
        meta['tagline'] = md.get('tagline', '')
        meta['cast'] = []
        meta['rating'] = md.get('rating', 0)
        meta['duration'] = str(md.get('runtime', 0))
        meta['plot'] = md.get('overview', '')
        meta['mpaa'] = md.get('certification', '')
        meta['premiered'] = md.get('released', year)
        meta['trailer_url'] = md.get('trailer', '')
        meta['genre'] = ''
        if md.has_key('imdb_genres'):
            meta['genre'] = md.get('imdb_genres', '')
        meta['studio'] = ''
        
        #Get cast, director, writers
        cast_list = []
        cast_list = md.get('cast','')
        if cast_list:
            for cast in cast_list:
                job=cast.get('job','')
                if job == 'Actor':
                    meta['cast'].append((cast.get('name',''),cast.get('character','') ))
                elif job == 'Director':
                    meta['director'] = cast.get('name','')
                elif job == 'Screenplay':
                    if meta['writer']:
                        meta['writer'] = meta['writer'] + ' / ' + cast.get('name','')
                    else:
                        meta['writer'] = cast.get('name','')
        else:
            meta['cast'] = ''
                    
        genre_list = []
        genre_list = md.get('genres', '')
        for genre in genre_list:
            if meta['genre'] == '':
                meta['genre'] = genre.get('name','')
            else:
                meta['genre'] = meta['genre'] + ' / ' + genre.get('name','')
        
        if md.has_key('tvdb_studios'):
            meta['studio'] = md.get('tvdb_studios', '')
        try:
            meta['studio'] = (md.get('studios', '')[0])['name']
        except:
            try:
                meta['studio'] = (md.get('studios', '')[1])['name']
            except:
                try:
                    meta['studio'] = (md.get('studios', '')[2])['name']
                except:
                    try:    
                        meta['studio'] = (md.get('studios', '')[3])['name']
                    except:
                        print 'Studios failed: %s ' % md.get('studios', '')
                        pass
        
        #set whether that database row will be accompanied by pre-packed images.
        meta['imgs_prepacked'] = self.classmode

        # define these early cuz they must exist whether posters do or not
        meta['thumb_url'] = ''
        meta['cover_url'] = ''
        if md.has_key('imdb_poster'):
            meta['cover_url'] = md.get('imdb_poster', '')
        if md.has_key('posters'):
            # find first thumb poster url
            for poster in md['posters']:
                if poster['image']['size'] == 'thumb':
                    meta['thumb_url'] = poster['image']['url']
                    break
            # find first cover poster url
            for poster in md['posters']:
                if poster['image']['size'] == 'cover':
                    meta['cover_url'] = poster['image']['url']
                    break

        meta['backdrop_url'] = ''
        if md.has_key('backdrops'):
            # find first original backdrop url
            for backdrop in md['backdrops']:
                if backdrop['image']['size'] == 'original':
                    meta['backdrop_url'] = backdrop['image']['url']
                    break

        return meta
        
            
    def get_episode_meta(self, imdb_id, season, episode):
        '''
        Requests meta data from TVDB for TV episodes, searches local cache db first.
        
        Args:
            imdb_id (str): IMDB ID
            season (str): tv show season number, number only no other characters
            episode (str): tv show episode number, number only no other characters
                        
        Returns:
            DICT. It must also return an empty dict when
            no meta info was found in order to save these.
        '''  
        
        # Add the tt if not found. integer aware.
        imdb_id=str(imdb_id)
        if not imdb_id.startswith('tt'):
                imdb_id = "tt%s" % imdb_id
        print "season is '" + season + "'"
        
        dateSearch = False
        searchTVDB = True
        
        #Find tvdb_id for the TVshow
        tvdb_id = self._get_tvdb_id(imdb_id)
        
        #Check if it exists in local cache first
        meta = self._cache_lookup_episode(imdb_id, season, episode)#ep_num)
        
        #If not found lets scrape online sources
        if meta is None:
            
            if tvdb_id == '' or tvdb_id is None:
                print "Could not find TVshow with imdb " + imdb_id
                
                meta = {}
                meta['code']=imdb_id
                meta['tvdb_id']=''
                meta['season']=season
                meta['season_num'] = 0
                meta['episode']=episode
                meta['episode_num'] = 0
                meta['episode_id'] = ''
                meta['title']= ''
                meta['tvdb_name'] = ''
                meta['plot'] = ''
                meta['rating'] = 0
                meta['aired'] = ''
                meta['poster'] = ''
                meta['cover_url']=meta['poster']
                meta['trailer_url']=''
                meta['premiered']=meta['aired']
                meta = self._get_tv_extra(meta)
                meta['overlay'] = self.get_watched_episode(meta)
                
                self._cache_save_episode_meta(meta)
                
                return meta
            
            if searchTVDB:
                meta = self._get_tvdb_episode_data(tvdb_id, season, episode, dateSearch)
                if meta is None:
                    meta = {}
                    meta['episode_id'] = ''
                    meta['tvdb_name'] = ''
                    meta['plot'] = ''
                    meta['rating'] = 0
                    meta['aired'] = ''
                    meta['poster'] = ''
                    meta['season_num'] = 0
                    meta['episode_num'] = 0
            else:
                meta = {}
                meta['episode_id'] = ''
                meta['tvdb_name'] = ''
                meta['plot'] = ''
                meta['rating'] = 0
                meta['aired'] = ''
                meta['poster'] = ''
                meta['season_num'] = 0
                meta['episode_num'] = 0
                
            #if meta is not None:
            meta['code']=imdb_id
            meta['tvdb_id']=tvdb_id
            meta['season']=season
            meta['episode']=episode
            meta['title']=meta['tvdb_name']
            meta['cover_url']=meta['poster']
            meta['trailer_url']=''
            meta['premiered']=meta['aired']
            meta = self._get_tv_extra(meta)
            meta['overlay'] = self.get_watched_episode(meta)
            self._cache_save_episode_meta(meta)
        
        else:
            print 'Episode found on db, meta='+str(meta)

        return meta
  
    
    def _get_tv_extra(self, meta):
        '''
        When requesting episode information, not all data may be returned
        Fill in extra missing meta information from tvshow_meta table which should
        have already been populated.
        
        Args:
            meta (dict): current meta dict
                        
        Returns:
            DICT containing the extra values
        '''     
        self.dbcur.execute("SELECT * FROM tvshow_meta WHERE imdb_id = '%s'" % meta['code']) #select database row where imdb_id matches
        matchedrow = self.dbcur.fetchone()

        if matchedrow:
            match = dict(matchedrow)
            meta['genre'] = match['genre']
            meta['duration'] = match['duration']
            meta['studio'] = match['studio']
            meta['mpaa'] = match['mpaa']
        else:
            meta['genre'] = ''
            meta['duration'] = '0'
            meta['studio'] = ''
            meta['mpaa'] = ''

        return meta


    def _get_tvdb_id(self, imdb_id):
        '''
        Retrieves TVID for a tv show that has already been scraped and saved in cache db.
        
        Used when scraping for season and episode data
        
        Args:
            imdb_id (str): IMDB ID
                        
        Returns:
            (str) imdb_id 
        '''      
        self.dbcur.execute("SELECT * FROM tvshow_meta WHERE imdb_id = '%s'" % imdb_id) #select database row where imdb_id matches
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
                return dict(matchedrow)['tmdb_id']
        else:
            return None

    
    def _cache_lookup_episode(self, imdb_id, season, episode):
        '''
        Lookup in local cache db for episode data
        
        Args:
            imdb_id (str): IMDB ID
            season (str): tv show season number, number only no other characters
            episode (str): tv show episode number, number only no other characters
                        
        Returns:
            DICT. Returns results found or None.
        ''' 
        print 'Looking up episode data in cache db, imdb id: %s season: %s episode: %s' % (imdb_id, season, episode) 
        self.dbcur.execute('SELECT '
                           'episode_meta.plot as plot, '
                           'tvshow_meta.genre as genre, '
                           'tvshow_meta.duration as duration, '
                           'episode_meta.aired as premiered, '
                           'tvshow_meta.studio as studio, '
                           'tvshow_meta.mpaa as mpaa, '
                           'episode_meta.imdb_id as code, '
                           'episode_meta.rating as rating, '
                           'episode_meta.season_num as season_num, '
                           'episode_meta.episode_num as episode_num, '
                           '"" as trailer_url, '
                           'episode_meta.season as season, '
                           'episode_meta.overlay as overlay, '
                           'episode_meta.poster as cover_url ' 
                           'FROM episode_meta, tvshow_meta WHERE '
                           'episode_meta.imdb_id = tvshow_meta.imdb_id AND '
                           'episode_meta.tvdb_id = tvshow_meta.tmdb_id AND '
                           'episode_meta.imdb_id = "%s" AND season = "%s" AND episode_meta.title = "%s" ' % (imdb_id, season, episode) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            print 'Match found: ', dict(matchedrow)
            return dict(matchedrow)
        else:
            return None
        

    def _get_tvdb_episode_data(self, tvdb_id, season, episode, dateSearch=False):
        '''
        Initiates lookup for episode data on TVDB
        
        Args:
            tvdb_id (str): TVDB id
            season (str): tv show season number, number only no other characters
            episode (str): tv show episode number, number only no other characters
            dateSearch (bool): search based on a date range
                        
        Returns:
            DICT. Data found from lookup
        '''      
        #get metadata text using themoviedb api
        tmdb = TMDB()
        meta = tmdb.tvdbLookup(tvdb_id,season,episode, dateSearch)      
        return meta

        
    def _cache_save_episode_meta(self, meta):
        '''
        Save episode data to local cache db.
        
        Args:
            meta (dict): episode data to be stored
                        
        '''      
        try: 
            self.dbcur.execute('SELECT * FROM episode_meta WHERE '
                               'imdb_id = "%s" AND tvdb_id = "%s" AND season = "%s" AND name = "%s"' 
                               % (meta['code'], meta['tvdb_id'], meta['season'], meta['name']) )
            matchedrow = self.dbcur.fetchone()
            if matchedrow:
                    print 'Episode matched row found, deleting table entry', matchedrow
                    self.dbcur.execute('DELETE FROM episode_meta WHERE '
                               'imdb_id = "%s" AND tvdb_id = "%s" AND season = "%s" AND title = "%s" ' 
                               % (meta['code'], meta['tvdb_id'], meta['season'], meta['title']) ) 
        except Exception, e:
            print 'Error attempting to delete from cache table: %s ' % e
            print 'Meta data:', meta
            pass        
        
        print 'Saving episode cache information: ', meta
        try:
            meta['imdb_id'] = meta['code']
            self.dbcur.execute("INSERT INTO episode_meta VALUES "
                               "(:imdb_id, :tvdb_id, :season, :season_num, :episode, :episode_num, :episode_id, :title, :tvdb_name, :plot, :rating, :aired, :poster, :overlay)",
                               meta
                               #"('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
                               #% ( meta['code'], meta['tmdb_id'],meta['title'],meta['rating'],meta['duration'],meta['plot'],meta['mpaa'],
                               #meta['premiered'],meta['genre'],meta['studio'],meta['thumb_url'],meta['cover_url'],meta['trailer_url'],meta['backdrop_url'],meta['imgs_prepacked'])
            )
            self.dbcon.commit()
        except Exception, e:
            print 'Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass        


    def change_watched(self, imdb_id, type, name, season=''):
        '''
        Change watched status on video
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'
            name (str): name of video
        Kwargs:            
            season (str): season number
                        
        '''    
        # add the tt if not found. integer aware.
        imdb_id=str(imdb_id)
        if not imdb_id.startswith('tt'):
            imdb_id = "tt%s" % imdb_id
                
        if type == 'movie' or type == 'tvshow':
            watched = self.get_watched(imdb_id, type)
            if watched == 6:
                self.update_watched(imdb_id, type, 7)
            else:
                self.update_watched(imdb_id, type, 6)
        elif type == 'episode':
            tvdb_id = self._get_tvdb_id(imdb_id)
            if tvdb_id is None:
                tvdb_id = ''
            tmp_meta = {}
            tmp_meta['code'] = imdb_id
            tmp_meta['tvdb_id'] = tvdb_id 
            tmp_meta['season']  = season
            tmp_meta['name']    = name
            watched = self.get_watched_episode(tmp_meta)
            if watched == 6:
                self.update_watched(imdb_id, type, 7, name=name, season=season, tvdb_id=tvdb_id)
            else:
                self.update_watched(imdb_id, type, 6, name=name, season=season, tvdb_id=tvdb_id)
                
    
    def update_watched(self, imdb_id, type, new_value, name='', season='', tvdb_id=''):
        '''
        Commits the DB update for the watched status
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'
            newvalue (str): value to update overlay field with
        Kwargs:
            name (str): name of video        
            season (str): season number
            tvdb_id (str): tvdb id of tvshow                        

        '''      
        if type == 'movie':
            sql="UPDATE movie_meta SET overlay = " + str(new_value) + " WHERE imdb_id = '" + imdb_id + "'" 
        elif type == 'tvshow':
            sql="UPDATE tvshow_meta SET overlay = " + str(new_value) + " WHERE imdb_id = '" + imdb_id + "'"
        elif type == 'episode':
            sql='UPDATE episode_meta SET overlay = ' + str(new_value) + ' WHERE imdb_id = "' + imdb_id + '" AND tvdb_id = "' + tvdb_id + '" AND season = "' + season + '" AND name = "' + name + '" '
            print sql
        else: # Something went really wrong
            return None
        
        self.dbcur.execute(sql)
        self.dbcon.commit()
    
   
    def get_watched(self, imdb_id, type):
        '''
        Finds the watched status of the video from the cache db
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'                    

        ''' 
        if type == 'movie':
            table='movie_meta'
        elif type == 'tvshow':
            table='tvshow_meta'
        
        self.dbcur.execute("SELECT * FROM " + table + " WHERE imdb_id = '%s'" % imdb_id)
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            return dict(matchedrow)['overlay']
        else:
            return 6

        
    def get_watched_episode(self, meta):
        '''
        Finds the watched status of the video from the cache db
        
        Args:
            meta (dict): full data of episode                    

        '''     
        self.dbcur.execute('SELECT * FROM episode_meta WHERE ' +
                           'imdb_id = "%s" AND tvdb_id = "%s" AND season = "%s" AND title = "%s" ' 
                           % (meta['code'], meta['tvdb_id'], meta['season'], meta['title']) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
                return dict(matchedrow)['overlay']
        else:
            return 6
    

    def find_cover(self, season, images):
        '''
        Finds the url of the banner to be used as the cover 
        from a list of images for a given season
        
        Args:
            season (str): tv show season number, number only no other characters
            images (dict): all images related
                        
        Returns:
            (str) cover_url: url of the selected image
        '''         
        cover_url = ''
        
        for image in images:
            (banner_url, banner_type, banner_season) = image
            if banner_season == season and banner_type == 'season':
                cover_url = banner_url
                break
        
        return cover_url
    

    def get_season_covers(self, imdb_id, seasons):
        '''
        Requests from TVDB a list of images for a given tvshow
        and list of seasons
        
        Args:
            imdb_id (str): IMDB ID
            seasons (str): a list of seasons, numbers only
                        
        Returns:
            (list) list of covers found for each season
        '''     
        # add the tt if not found. integer aware.
        imdb_id=str(imdb_id)
        if not imdb_id.startswith('tt'):
                imdb_id = "tt%s" % imdb_id
                
        coversList = []
        tvdb_id = self._get_tvdb_id(imdb_id)
        images  = None
        for season in seasons:
            meta = self._cache_lookup_season(imdb_id, season)
            if meta is None:
                meta = {}
                if tvdb_id is None or tvdb_id == '':
                    meta['cover_url']=''
                elif images:
                    meta['cover_url']=self.find_cover(season, images )
                else:
                    if len(season) == 4:
                        meta['cover_url']=''
                    else:
                        tmdb = TMDB()
                        images = tmdb.getSeasonPosters(tvdb_id, season)
                        print images
                        meta['cover_url']=self.find_cover(season, images )
                        
                meta['season']=season
                meta['tvdb_id'] = tvdb_id
                meta['code'] = imdb_id
                meta['overlay'] = 6
                
                self._cache_save_season_meta(meta)
            
            coversList.append(meta)
            
        return coversList
    

    def _cache_lookup_season(self, imdb_id, season):
        '''
        Lookup data for a given season in the local cache DB.
        
        Args:
            imdb_id (str): IMDB ID
            season (str): tv show season number, number only no other characters
                        
        Returns:
            (dict) meta data for a match
        '''      
        self.dbcur.execute("SELECT * FROM season_meta WHERE imdb_id = '%s' AND season ='%s' " 
                           % ( imdb_id, season ) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            return dict(matchedrow)
        else:
            return None
    

    def _cache_save_season_meta(self, meta):
        '''
        Save data for a given season in local cache DB.
        
        Args:
            meta (dict): full meta data for season
        '''     
        try:
            self.dbcur.execute("SELECT * FROM season_meta WHERE imdb_id = '%s' AND season ='%s' " 
                               % ( meta['code'], meta['season'] ) ) 
            matchedrow = self.dbcur.fetchone()
            if matchedrow:
                print 'Season matched row found, deleting table entry', matchedrow
                self.dbcur.execute("DELETE FROM season_meta WHERE imdb_id = '%s' AND season ='%s' " 
                                   % ( meta['code'], meta['season'] ) )
        except Exception, e:
            print 'Error attempting to delete from cache table: %s ' % e
            print 'Meta data:', meta
            pass 
                    
        print 'Saving season cache information: ', meta
        try:
            meta['imdb_id'] = meta['code']
            self.dbcur.execute("INSERT INTO season_meta VALUES "
                               "(:imdb_id, :tvdb_id, :season, :cover_url, :overlay)",
                               meta
                               )
            self.dbcon.commit()
        except Exception, e:
            print 'Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass         
    

    def refresh_movie_meta(self, imdb_id):

        # add the tt if not found. integer aware.
        imdb_id=str(imdb_id)
        if not imdb_id.startswith('tt'):
                imdb_id = "tt%s" % imdb_id

        meta = None #self._cache_lookup_by_imdb(imdb_id)

        if meta is None:
            #print "adding to cache and getting metadata from web"
            meta = self._get_tmdb_meta(imdb_id)
            self._cache_save_video_meta(meta)

            #if creating a metadata container, download the images.
            if self.classmode == 'true':
                self._downloadimages(meta,'movies',imdb_id)

        if meta is not None:

            #if cache row says there are pre-packed images,..
            if meta['imgs_prepacked'] == 'true':

                    #define the image paths
                    cover_path=os.path.join(self.mvcovers,imdb_id,self._picname(meta['cover_url']))
                    #backdrop_path=os.path.join(self.mvbackdrops,imdb_id,self._picname(meta['backdrop_url']))

                    #if paths exist, replace the urls with paths
                    if self.classmode == 'false':
                        if os.path.exists(cover_path):
                            meta['cover_url'] = cover_path
                        #if os.path.exists(backdrop_path):
                        #    meta['backdrop_url'] = backdrop_path

                    #try some image redownloads if building container
                    elif self.classmode == 'true':
                        if not os.path.exists(cover_path):
                                self._downloadimages(meta,'movies',imdb_id)

                        #if not os.path.exists(backdrop_path):
                        #        self._downloadimages(meta,'movies',imdb_id)
           
        return meta
       
