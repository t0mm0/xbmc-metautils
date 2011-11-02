'''
    These classes cache metadata from TheMovieDB and TVDB.
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
import xbmc, xbmcaddon
from datetime import datetime
import time
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
            
        #set movie/tvshow constants
        self.type_movie = 'movie'
        self.type_tvshow = 'tvshow'
        self.type_episode = 'episode'
            
        #this init auto-constructs necessary folder hierarchies.

        # control whether class is being used to prepare pre-packaged .zip
        self.classmode = bool2string(preparezip)
        self.videocache = os.path.join(self.cache_path, 'video_cache.db')

        self.tvpath = make_dir(self.cache_path, self.type_tvshow)
        self.tvcovers = make_dir(self.tvpath, 'covers')
        self.tvbackdrops = make_dir(self.tvpath, 'backdrops')

        self.mvpath = make_dir(self.cache_path, self.type_movie)
        self.mvcovers = make_dir(self.mvpath, 'covers')
        self.mvbackdrops = make_dir(self.mvpath, 'backdrops')

        # connect to db at class init and use it globally
        self.dbcon = sqlite.connect(self.videocache)
        self.dbcon.row_factory = sqlite.Row # return results indexed by field names and not numbers so we can convert to dict
        self.dbcur = self.dbcon.cursor()

        # initialize cache db
        self._cache_create_movie_db()


    def __del__(self):
        ''' Cleanup db when object destroyed '''
        self.dbcur.close()
        self.dbcon.close()


    def _cache_create_movie_db(self):
        ''' Creates the cache tables if they do not exist.  '''   
                        
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS movie_meta ("
                           "imdb_id TEXT, tmdb_id TEXT, title TEXT, year INTEGER,"
                           "director TEXT, writer TEXT, tagline TEXT, cast TEXT,"
                           "rating FLOAT, duration TEXT, plot TEXT,"
                           "mpaa TEXT, premiered TEXT, genre TEXT, studio TEXT,"
                           "thumb_url TEXT, cover_url TEXT,"
                           "trailer_url TEXT, backdrop_url TEXT,"
                           "imgs_prepacked TEXT," # 'true' or 'false'. added to determine whether to load imgs from path not url (ie. if they are included in pre-packaged metadata container).
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, tmdb_id, title, year)"
                           ");"
        )
        self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on movie_meta (title);')
        print 'Table movie_meta initialized'
        
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS tvshow_meta ("
                           "imdb_id TEXT, tvdb_id TEXT, title TEXT, cast TEXT,"
                           "rating FLOAT, duration TEXT, plot TEXT,"
                           "mpaa TEXT, premiered TEXT, genre TEXT, studio TEXT,"
                           "thumb_url TEXT, cover_url TEXT,"
                           "trailer_url TEXT, backdrop_url TEXT,"
                           "imgs_prepacked TEXT," # 'true' or 'false'. added to determine whether to load imgs from path not url (ie. if they are included in pre-packaged metadata container).
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, tvdb_id, title)"
                           ");"
        )
        self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on tvshow_meta (title);')
        print 'Table tvshow_meta initialized'

        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS season_meta ("
                           "imdb_id TEXT, "
                           "tvdb_id TEXT, " 
                           "season TEXT, "
                           "cover_url TEXT,"
                           "overlay INTEGER,"
                           "UNIQUE(imdb_id, tvdb_id, season)"
                           ");"
        )
               
        #self.dbcur.execute('CREATE INDEX IF NOT EXISTS nameindex on tvshow_meta (name);')
        print 'Table season_meta initialized'
                
        # split text across lines to make it easier to understand
        self.dbcur.execute("CREATE TABLE IF NOT EXISTS episode_meta ("
                           "imdb_id TEXT, "
                           "tvdb_id TEXT, "
                           "episode_id TEXT, "                           
                           "season INTEGER, "
                           "episode INTEGER, "
                           "title TEXT, "
                           "director TEXT, "
                           "writer TEXT, "
                           "plot TEXT, "
                           "rating FLOAT, "
                           "premiered TEXT, "
                           "poster TEXT, "
                           "overlay INTEGER, "
                           "UNIQUE(imdb_id, tvdb_id, episode_id, title)"
                           ");"
        )
        print 'Table episode_meta initialized'


    def _init_tvshow_meta(self, imdb_id, tvdb_id, name):
        '''
        Initializes a tvshow_meta dictionary with default values, to ensure we always
        have all fields
        
        Args:
            imdb_id (str): IMDB ID
            name (str): full name of movie you are searching
            premiered (str): 10 digit year YYYY-MM-DD
                        
        Returns:
            DICT in the structure of what is required to write to the DB
        '''          
        meta = {}
        meta['imdb_id'] = imdb_id
        meta['tvdb_id'] = tvdb_id
        meta['title'] = name
        meta['rating'] = 0
        meta['duration'] = 0
        meta['plot'] = ''
        meta['mpaa'] = ''
        meta['premiered'] = ''
        meta['trailer_url'] = ''
        meta['genre'] = ''
        meta['studio'] = ''
        meta['cast'] = []
        meta['thumb_url'] = ''
        
        #set whether that database row will be accompanied by pre-packed images.
        meta['imgs_prepacked'] = self.classmode
        
        meta['cover_url'] = ''
        meta['backdrop_url'] = ''
        meta['overlay'] = 6
        return meta


    def _init_movie_meta(self, imdb_id, tmdb_id, name, year):
        '''
        Initializes a movie_meta dictionary with default values, to ensure we always
        have all fields
        
        Args:
            imdb_id (str): IMDB ID
            name (str): full name of movie you are searching
            premiered (str): 10 digit year YYYY-MM-DD
                        
        Returns:
            DICT in the structure of what is required to write to the DB
        '''                
        if year:
            year = int(year)
            
        meta = {}
        meta['imdb_id'] = imdb_id
        meta['tmdb_id'] = tmdb_id
        meta['title'] = name
        meta['year'] = year
        meta['writer'] = ''
        meta['director'] = ''
        meta['tagline'] = ''
        meta['cast'] = []
        meta['rating'] = 0
        meta['duration'] = 0
        meta['plot'] = ''
        meta['mpaa'] = ''
        meta['premiered'] = ''
        meta['trailer_url'] = ''
        meta['genre'] = ''
        meta['studio'] = ''
        
        #set whether that database row will be accompanied by pre-packed images.                        
        meta['imgs_prepacked'] = self.classmode
        
        meta['thumb_url'] = ''
        meta['cover_url'] = ''
        meta['backdrop_url'] = ''
        meta['overlay'] = 6
        return meta
    
    
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
 

    def _convert_date(self, string, in_format, out_format):
        ''' Helper method to convert a string date to a given format '''
        
        #Legacy check, Python 2.4 does not have strptime attribute, instroduced in 2.5
        #if hasattr(datetime, 'strptime'):
        #    strptime = datetime.strptime
        #else:
        #    strptime = lambda date_string, format: datetime(*(time.strptime(date_string, format)[0:6]))
        
        strptime = lambda date_string, format: datetime(*(time.strptime(date_string, format)[0:6]))
        try:
            a = strptime(string, in_format).strftime(out_format)
        except Exception, e:
            print '************* Error Date conversion failed: %s' % e
            return None
        return a
        
        
    def _get_date(self, year, month_day):
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
               
        return year + '-' + month + '-' + day   
    
    
    def _downloadimages(self,meta,mediatype,name):
        '''
        Download images to save locally
        
        Args:
            meta (dict): meta data dict
            mediatype (str): 'movies' or 'tvshow'
            name (str): filename to download
        '''                 
        

        if mediatype==self.type_movie:
             cover_folder=os.path.join(self.mvcovers, name)
             backdrop_folder=os.path.join(self.mvbackdrops, name)
        elif mediatype==self.type_tvshow:
             cover_folder = os.path.join(self.tvcovers, name)
             backdrop_folder=os.path.join(self.tvbackdrops, name)

    
        if not os.path.exists(cover_folder):
            os.makedirs(cover_folder)

        cover_name=self._picname(meta['cover_url'])
        cover_path = os.path.join(cover_folder, cover_name)

        self._dl_code(meta['cover_url'],cover_path)
        
        backdrop_name=self._picname(meta['backdrop_url'])
        backdrop_path = os.path.join(backdrop_folder, backdrop_name)
      
        self._dl_code(meta['backdrop_url'],backdrop_path)              

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
      

    def get_meta(self, type, name, imdb_id='', tmdb_id='', year=''):
        '''
        Main method to get meta data for movie or tvshow. Will lookup by name/year 
        if no IMDB ID supplied.       
        
        Args:
            type (str): 'movie' or 'tvshow'
            name (int): full name of movie/tvshow you are searching            
        Kwargs:
            imdb_id (str): IMDB ID        
            tmdb_id (str): TMDB ID
            year (str): 4 digit year of video, recommended to include the year whenever possible
                        to maximize correct search results.
                        
        Returns:
            DICT of meta data or None if cannot be found.
        '''
        
        print '---------------------------------------------------------------------------------------'
        print 'Attempting to retreive meta data for %s: %s %s %s %s' % (type, name, year, imdb_id, tmdb_id)
        
        if imdb_id:
            # add the tt if not found. integer aware.
            imdb_id=str(imdb_id)
            if not imdb_id.startswith('tt'):
                imdb_id = "tt%s" % imdb_id

            meta = self._cache_lookup_by_id(type, imdb_id=imdb_id)
        elif tmdb_id:
            meta = self._cache_lookup_by_id(type, tmdb_id=tmdb_id)
        else:
            meta = self._cache_lookup_by_name(type, name, year)

        if not meta:
            
            if type==self.type_movie:
                meta = self._get_tmdb_meta(imdb_id, tmdb_id, name, year)
            elif type==self.type_tvshow:
                meta = self._get_tvdb_meta(imdb_id, name, year)
                       
            self._cache_save_video_meta(meta, name, type)

            #if creating a metadata container, download the images.
            if self.classmode == 'true':
                self._downloadimages(meta,type,imdb_id)

        if meta:

            #Change cast back into a tuple
            if meta['cast']:
                meta['cast'] = eval(meta['cast'])
                
            #Return a trailer link that will play via youtube addon
            try:
                trailer_id = re.match('^[^v]+v=(.{3,11}).*', meta['trailer_url']).group(1)
                meta['trailer'] = 'plugin://plugin.video.youtube/?action=play_video&videoid=%s' % trailer_id
            except:
                meta['trailer'] = ''
            
            #if cache row says there are pre-packed images then either use them or create them
            if meta['imgs_prepacked'] == 'true':

                    #define the image paths
                    if type == self.type_movie:
                        cover_path = os.path.join(self.mvcovers, imdb_id, self._picname(meta['cover_url']))
                        backdrop_path=os.path.join(self.mvbackdrops,imdb_id,self._picname(meta['backdrop_url']))
                    elif type == self.type_tvshow:
                        cover_path = os.path.join(self.tvcovers, imdb_id, self._picname(meta['cover_url']))
                        backdrop_path=os.path.join(self.tvbackdrops,imdb_id,self._picname(meta['backdrop_url']))
                    

                    #if paths exist, replace the urls with paths
                    if self.classmode == 'false':
                        if os.path.exists(cover_path):
                            meta['cover_url'] = cover_path
                        if os.path.exists(backdrop_path):
                            meta['backdrop_url'] = backdrop_path

                    #try some image redownloads if building container
                    elif self.classmode == 'true':
                        if not os.path.exists(cover_path):
                                self._downloadimages(meta,type,imdb_id)

                        if not os.path.exists(backdrop_path):
                                self._downloadimages(meta,'movies',imdb_id)
        
        #We want to send back the name that was passed in   
        meta['title'] = name
        
        return meta  


    def update_meta(self, name, old_imdb_id, old_tmdb_id, new_imdb_id='', new_tmdb_id='', year=''):
        '''
        Updates and returns meta data for given movie, 
        mainly to be used with refreshing individual movies.
        
        Searches local cache DB for record, delete if found, calls get_meta() to grab new data
               
        
        Args:
            name (int): full name of movie you are searching            
        Kwargs:
            imdb_id (str): IMDB ID        
            tmdb_id (str): TMDB ID
            year (str): 4 digit year of video, recommended to include the year whenever possible
                        to maximize correct search results.
                        
        Returns:
            DICT of meta data or None if cannot be found.
        '''
        print '---------------------------------------------------------------------------------------'
        print 'Updating meta data: %s Old: %s %s New: %s %s Year: %s' % (name, old_imdb_id, old_tmdb_id, new_imdb_id, new_tmdb_id, year)
        
        if old_imdb_id:
            meta = self._cache_lookup_by_id('movie', imdb_id=old_imdb_id)
        elif old_tmdb_id:
            meta = self._cache_lookup_by_id('movie', tmdb_id=old_tmdb_id)
        else:
            meta = self._cache_lookup_by_name('movie', name, year)
        
        if meta:
            self._cache_delete_video_meta(old_imdb_id, old_tmdb_id, name, year)
        else:
            print 'No match found in cache db'
            
        return self.get_meta(self.type_movie, name, new_imdb_id, new_tmdb_id, year)
        

    def _cache_lookup_by_id(self, type, imdb_id='', tmdb_id=''):
        '''
        Lookup in SQL DB for video meta data by IMDB ID
        
        Args:
            imdb_id (str): IMDB ID
            type (str): 'movie' or 'tvshow'
        Kwargs:
            imdb_id (str): IDMB ID
            tmdb_id (str): TMDB ID                        
        Returns:
            DICT of matched meta data or None if no match.
        '''        
        if type == self.type_movie:
            table='movie_meta'
        elif type == self.type_tvshow:
            table='tvshow_meta'

        if imdb_id:
            sql_select = "SELECT * FROM %s WHERE imdb_id = '%s'" % (table, imdb_id)  
        else:
            sql_select = "SELECT * FROM %s WHERE tmdb_id = '%s'" % (table, tmdb_id)  
        
        print 'Looking up in local cache by id for: %s %s %s' % (type, imdb_id, tmdb_id)
        print 'SQL Select: %s' % sql_select        
        try:    
            self.dbcur.execute(sql_select)
            matchedrow = self.dbcur.fetchone()            
        except Exception, e:
            print '************* Error selecting from cache db: %s' % e
            return None
        
        if matchedrow:
            print 'Found meta information by id in cache table: ', dict(matchedrow)
            return dict(matchedrow)
        else:
            print 'No match in local DB'
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
        if type == self.type_movie:
            table='movie_meta'
        elif type == self.type_tvshow:
            table='tvshow_meta'
        
        name =  self._clean_string(name.lower())
        print 'Looking up in local cache by name for: %s %s %s' % (type, name, year)
        
        sql_select = "SELECT * FROM %s WHERE title = '%s'" % (table, name)
        if year:
            sql_select = sql_select + " AND year = %s" % year
        print 'SQL Select: %s' % sql_select            
        
        try:
            self.dbcur.execute(sql_select)            
            matchedrow = self.dbcur.fetchone()
        except Exception, e:
            print '************* Error attempting to select from table: %s with error: %s' % (table, e)
            pass
            
        if matchedrow:
            print 'Found meta information by name in cache table: ', dict(matchedrow)
            return dict(matchedrow)
        else:
            print 'No match in local DB'            
            return None
                       

    def _cache_save_video_meta(self, meta, name, type):
        '''
        Saves meta data to SQL table given type
        
        Args:
            meta (dict): meta data of video to be added to database
            type (str): 'movie' or 'tvshow'
                        
        '''            
        if type == self.type_movie:
            table='movie_meta'
        elif type == self.type_tvshow:
            table='tvshow_meta'
        
        #strip title
        meta['title'] =  self._clean_string(name.lower())
        
        #Select on either IMDB ID or name + premiered
        if meta['imdb_id']:
            sql_select = "SELECT * FROM %s WHERE imdb_id = '%s'" % (table, meta['imdb_id'])
        else:           
            sql_select = "SELECT * FROM %s WHERE title = '%s'" % (table, meta['title'])
            if meta['year']:
                sql_select = sql_select + " AND year = '%s'" % meta['year']

        print 'Checking if entry already exists in cache table: %s' % table
        print 'SQL SELECT: %s' % sql_select            
        
        try:          
            self.dbcur.execute(sql_select) #select database row
            matchedrow = self.dbcur.fetchone()
        except Exception, e:
            print '************* Error attempting to select from table: %s with error: %s' % (table, e)
            pass
            
        if matchedrow:
            print 'Matched Row found, deleting table entry'
            sql_delete = "DELETE FROM %s WHERE imdb_id = '%s'" % (table, meta['imdb_id'])
            print 'SQL DELETE: %s' % sql_delete       
            
            try:
                self.dbcur.execute(sql_delete)
            except Exception, e:
                print '************* Error attempting to delete from cache table: %s ' % e
                print 'Meta data:', meta               
                pass
        
        if meta.has_key('cast'):
            meta['cast'] = str(meta['cast'])
        
        print 'Saving cache information: ', meta         
        try:
            if type == self.type_movie:
                self.dbcur.execute("INSERT INTO " + table + " VALUES "
                                   "(:imdb_id, :tmdb_id, :title, :year, :director, :writer, :tagline, :cast, :rating, :duration, :plot, :mpaa, :premiered, :genre, :studio, :thumb_url, :cover_url, :trailer_url, :backdrop_url, :imgs_prepacked, :overlay)",
                                   meta
                )
            elif type == self.type_tvshow:
                self.dbcur.execute("INSERT INTO " + table + " VALUES "
                                   "(:imdb_id, :tvdb_id, :title, :cast, :rating, :duration, :plot, :mpaa, :premiered, :genre, :studio, :thumb_url, :cover_url, :trailer_url, :backdrop_url, :imgs_prepacked, :overlay)",
                                   meta
                )
            self.dbcon.commit()
        except Exception, e:
            print '************* Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass            
    
    
    def _cache_delete_video_meta(self, imdb_id, tmdb_id, name, year):
        '''
        Delete meta data from SQL table
        
        Args:
            imdb_id (str): IMDB ID
            tmdb_id (str): TMDB ID   
            name (str): Full movie name
            year (int): Movie year
                        
        '''         
        if imdb_id:
            sql_delete = "DELETE FROM movie_meta WHERE imdb_id = '%s'" % imdb_id
        elif tmdb_id:
            sql_delete = "DELETE FROM movie_meta WHERE tmdb_id = '%s'" % tmdb_id
        else:
            name =  self._clean_string(name.lower())
            sql_delete = "DELETE FROM movie_meta WHERE title = '%s'" % name
            if year:
                sql_delete = sql_delete + ' AND year = %s' % (year)
               
        try:
            print 'Deleting table entry: %s %s %s %s ' % (imdb_id, tmdb_id, name, year)
            print 'SQL DELETE: %s' % sql_delete
            self.dbcur.execute(sql_delete)
        except Exception, e:
            print '************* Error attempting to delete from cache table: %s ' % e          
            pass    
        

    def _get_tmdb_meta(self, imdb_id, tmdb_id, name, year=''):
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
        meta = tmdb.tmdb_lookup(name,imdb_id,tmdb_id, year)       
        
        if meta is None:
            # create an empty dict so below will at least populate empty data for the db insert.
            meta = {}
 
        return self._format_tmdb_meta(meta, imdb_id, name, year)
    

    def _format_tmdb_meta(self, md, imdb_id, name, year):
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
        
        #Intialize movie_meta dictionary    
        meta = self._init_movie_meta(imdb_id, md.get('id', ''), name, year)
        
        meta['imdb_id'] = md.get('imdb_id', imdb_id)
        meta['title'] = md.get('name', name)      
        meta['tagline'] = md.get('tagline', '')
        meta['rating'] = md.get('rating', 0)
        meta['duration'] = str(md.get('runtime', 0))
        meta['plot'] = md.get('overview', '')
        meta['mpaa'] = md.get('certification', '')       
        meta['premiered'] = md.get('released', '')    
        
        #Do whatever we can to set a year, if we don't have one lets try to strip it from premiered
        if not year and meta['premiered']:
            meta['year'] = int(self._convert_date(meta['premiered'], '%Y-%m-%d', '%Y'))
                   
        meta['trailer_url'] = md.get('trailer', '')
        meta['genre'] = md.get('genre', '')
        
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
        
        meta['cover_url'] = md.get('cover_url', '')
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

        if md.has_key('backdrops'):
            # find first original backdrop url
            for backdrop in md['backdrops']:
                if backdrop['image']['size'] == 'original':
                    meta['backdrop_url'] = backdrop['image']['url']
                    break

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
        
        if imdb_id:
            tvdb_id = tvdb.get_show_by_imdb(imdb_id)

        #Intialize tvshow meta dictionary
        meta = self._init_tvshow_meta(imdb_id, tvdb_id, name)

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
                meta['imdb_id'] = imdb_id
                meta['tvdb_id'] = tvdb_id
                meta['title'] = show.name
                if str(show.rating) != '' and show.rating != None:
                    meta['rating'] = show.rating
                meta['duration'] = show.runtime
                meta['plot'] = show.overview
                meta['mpaa'] = show.content_rating
                meta['premiered'] = show.first_aired
                if show.genre != '':
                    temp = show.genre.replace("|",",")
                    temp = temp[1:(len(temp)-1)]
                    meta['genre'] = temp
                meta['studio'] = show.network
                if show.actors:
                    for actor in show.actors:
                        meta['cast'].append(actor)
                meta['thumb_url'] = show.banner_url
                meta['imgs_prepacked'] = self.classmode
                meta['cover_url'] = show.poster_url
                meta['backdrop_url'] = show.fanart_url
                meta['overlay'] = 6

                if meta['plot'] == 'None' or meta['plot'] == '' or meta['plot'] == 'TBD' or meta['plot'] == 'No overview found.' or meta['rating'] == 0 or meta['duration'] == 0 or meta['cast'] == '' or meta['cover_url'] == '':
                    print ' Some info missing in TVdb for TVshow *** '+ name + ' ***. Will search imdb for more'                    
                    tmdb = TMDB()
                    imdb_meta = tmdb.search_imdb(name, imdb_id)
                    if imdb_meta:
                        meta = tmdb.update_imdb_meta(meta, imdb_meta)

                return meta
            else:
                tmdb = TMDB()
                imdb_meta = tmdb.search_imdb(name, imdb_id)
                if imdb_meta:
                    meta = tmdb.update_imdb_meta(meta, imdb_meta)
                return meta    
        else:
            return meta

    
    def search_movies(self, name):
        '''
        Requests meta data from TMDB for any movie matching name
        
        Args:
            name (str): full name of movie you are searching
                        
        Returns:
            Arry of dictionaries with trimmed down meta data, only returned data that is required:
            - IMDB ID
            - TMDB ID
            - Name
            - Year
        ''' 
        print '---------------------------------------------------------------------------------------'
        print 'Meta data refresh - searching for movie: %s' % name
        tmdb = TMDB()
        movie_list = []
        meta = tmdb.tmdb_search(name)
        if meta:
            for movie in meta:
                if movie['released']:
                    year = self._convert_date(movie['released'], '%Y-%m-%d', '%Y')
                else:
                    year = None
                movie_list.append({'title': movie['name'], 'imdb_id': movie['imdb_id'], 'tmdb_id': movie['id'], 'year': year})
        else:
            print 'No results found'
            return None
 
        print 'Returning results: %s' % movie_list
        return movie_list

            
    def get_episode_meta(self, imdb_id, season, episode):
        '''
        Requests meta data from TVDB for TV episodes, searches local cache db first.
        
        Args:
            imdb_id (str): IMDB ID
            season (int): tv show season number, number only no other characters
            episode (int): tv show episode number, number only no other characters
                        
        Returns:
            DICT. It must also return an empty dict when
            no meta info was found in order to save these.
        '''  
        print '---------------------------------------------------------------------------------------'
        print 'Attempting to retreive episode meta data for: %s %s %s' % (imdb_id, season, episode)
               
        dateSearch = False
        searchTVDB = True
        
        #Find tvdb_id for the TVshow
        tvdb_id = self._get_tvdb_id(imdb_id)
        
        #Check if it exists in local cache first
        meta = self._cache_lookup_episode(imdb_id, tvdb_id, season, episode)
        
        #If not found lets scrape online sources
        if meta is None:
            
            if tvdb_id == '' or tvdb_id is None:
                print "Could not find TVshow with imdb " + imdb_id
                
                meta = {}
                meta['imdb_id']=imdb_id
                meta['tvdb_id']=''
                meta['episode_id'] = ''                
                meta['season']=season
                meta['episode']=episode
                meta['title']= ''
                meta['plot'] = ''
                meta['director'] = ''
                meta['writer'] = ''
                meta['rating'] = 0
                meta['premiered'] = ''
                meta['poster'] = ''
                meta['cover_url']=meta['poster']
                meta['trailer_url']=''
                meta['premiered']=meta['premiered']
                meta = self._get_tv_extra(meta)
                meta['overlay'] = self._get_watched_episode(meta)
                
                self._cache_save_episode_meta(meta)
                
                return meta
            
            if searchTVDB:
                meta = self._get_tvdb_episode_data(tvdb_id, season, episode, dateSearch)
                if meta is None:
                    meta = {}
                    meta['episode_id'] = ''
                    meta['plot'] = ''
                    meta['rating'] = 0
                    meta['premiered'] = ''
                    meta['poster'] = ''
                    meta['season'] = 0
                    meta['episode'] = 0
            else:
                meta = {}
                meta['episode_id'] = ''
                meta['plot'] = ''
                meta['rating'] = 0
                meta['premiered'] = ''
                meta['poster'] = ''
                meta['season'] = 0
                meta['episode'] = 0
                
            #if meta is not None:
            meta['imdb_id']=imdb_id
            meta['tvdb_id']=tvdb_id
            meta['season']=int(season)
            meta['episode']=int(episode)
            meta['cover_url']=meta['poster']
            meta['trailer_url']=''
            meta['premiered']=meta['premiered']
            meta = self._get_tv_extra(meta)
            meta['overlay'] = self._get_watched_episode(meta)
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
        sql_select = "SELECT * FROM tvshow_meta WHERE imdb_id = '%s'" % meta['imdb_id']
        print 'Retrieving extra TV Show information from tvshow_meta'
        print 'SQL SELECT: %s' % sql_select
        
        try:     
            self.dbcur.execute(sql_select)
            matchedrow = self.dbcur.fetchone()
        except Exception, e:
            print '************* Error attempting to select from tvshow_meta table: %s ' % e          
            pass   

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
        sql_select = "SELECT * FROM tvshow_meta WHERE imdb_id = '%s'" % imdb_id
        print 'Retrieving TVDB ID'
        print 'SQL SELECT: %s' % sql_select
        
        try:
            self.dbcur.execute(sql_select)
            matchedrow = self.dbcur.fetchone()
        except Exception, e:
            print '************* Error attempting to select from tvshow_meta table: %s ' % e          
            pass
                        
        if matchedrow:
                return dict(matchedrow)['tvdb_id']
        else:
            return None

    
    def _cache_lookup_episode(self, imdb_id, tvdb_id, season, episode):
        '''
        Lookup in local cache db for episode data
        
        Args:
            imdb_id (str): IMDB ID
            tvdb_id (str): TheTVDB ID
            season (str): tv show season number, number only no other characters
            episode (str): tv show episode number, number only no other characters
                        
        Returns:
            DICT. Returns results found or None.
        ''' 
        print 'Looking up episode data in cache db, imdb id: %s season: %s episode: %s' % (imdb_id, season, episode) 
        
        self.dbcur.execute('SELECT '
                           'episode_meta.title as title, '
                           'episode_meta.plot as plot, '
                           'episode_meta.director as director, '
                           'episode_meta.writer as writer, '
                           'tvshow_meta.genre as genre, '
                           'tvshow_meta.duration as duration, '
                           'episode_meta.premiered as premiered, '
                           'tvshow_meta.studio as studio, '
                           'tvshow_meta.mpaa as mpaa, '
                           'episode_meta.imdb_id as imdb_id, '
                           'episode_meta.rating as rating, '
                           '"" as trailer_url, '
                           'episode_meta.season as season, '
                           'episode_meta.episode as episode, '
                           'episode_meta.overlay as overlay, '
                           'episode_meta.poster as cover_url ' 
                           'FROM episode_meta, tvshow_meta WHERE '
                           'episode_meta.imdb_id = tvshow_meta.imdb_id AND '
                           'episode_meta.tvdb_id = tvshow_meta.tvdb_id AND '
                           'episode_meta.imdb_id = "%s" AND episode_meta.tvdb_id = "%s" AND season = %s AND episode_meta.episode = %s ' % (imdb_id, tvdb_id, season, episode) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            print 'Found episode meta information in cache table: ', dict(matchedrow)
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
        meta = self._tvdb_lookup(tvdb_id,season,episode, dateSearch)      
        return meta


    def _tvdb_lookup(self, tvdb_id, season_num, episode_num, dateSearch):
        #TvDB Lookup for episodes
        
        meta = {}
        tvdb = TheTVDB()
        if dateSearch:
            aired=self._get_date(season_num, episode_num)
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
        meta['plot'] = self._check(episode.overview)
        if episode.guest_stars:
            guest_stars = episode.guest_stars.replace('|', ', ')
            meta['plot'] = meta['plot'] + '\n\nGuest Starring: ' + guest_stars
        meta['rating'] = float(self._check(episode.rating,0))
        meta['premiered'] = self._check(episode.first_aired)
        meta['title'] = self._check(episode.name)
        meta['poster'] = self._check(episode.image)
        meta['director'] = self._check(episode.director)
        meta['writer'] = self._check(episode.writer)
        meta['season'] = int(self._check(episode.season_number,0))
        meta['episode'] = int(self._check(episode.episode_number,0))
              
        return meta


    def _check(self, value, ret=None):
        if value is None or value == '':
            if ret == None:
                return ''
            else:
                return ret
        else:
            return value
            
        
    def _cache_save_episode_meta(self, meta):
        '''
        Save episode data to local cache db.
        
        Args:
            meta (dict): episode data to be stored
                        
        '''      
        try: 
            self.dbcur.execute('SELECT * FROM episode_meta WHERE '
                               'imdb_id = "%s" AND tvdb_id = "%s" AND season = %s AND episode = %s '
                               % (meta['imdb_id'], meta['tvdb_id'], meta['season'], meta['episode']) )
            matchedrow = self.dbcur.fetchone()
            if matchedrow:
                    print 'Episode matched row found, deleting table entry'
                    self.dbcur.execute('DELETE FROM episode_meta WHERE '
                               'imdb_id = "%s" AND tvdb_id = "%s" AND season = %s AND episode = %s ' 
                               % (meta['imdb_id'], meta['tvdb_id'], meta['season'], meta['episode']) ) 
        except Exception, e:
            print '************* Error attempting to delete from cache table: %s ' % e
            print 'Meta data:', meta
            pass        
        
        print 'Saving episode cache information: ', meta
        try:
            self.dbcur.execute("INSERT INTO episode_meta VALUES "
                               "(:imdb_id, :tvdb_id, :episode_id, :season, :episode, :title, :director, :writer, :plot, :rating, :premiered, :poster, :overlay)",
                               meta
            )
            self.dbcon.commit()
        except Exception, e:
            print '************* Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass        


    def change_watched(self, type, name, imdb_id, tmdb_id='', season='', episode='', year=''):
        '''
        Change watched status on video
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'
            name (str): name of video
        Kwargs:            
            season (str): season number
                        
        '''   
        print '---------------------------------------------------------------------------------------'
        print 'Updating watched flag for: %s %s %s %s %s %s' % (type, name, imdb_id, tmdb_id, season, year)
                
        if type == self.type_movie or type == self.type_tvshow:
            watched = self._get_watched(imdb_id, type)
            if watched == 6:
                self._update_watched(imdb_id, type, 7, tmdb_id=tmdb_id)
            else:
                self._update_watched(imdb_id, type, 6, tmdb_id=tmdb_id)
        elif type == self.type_episode:
            tvdb_id = self._get_tvdb_id(imdb_id)
            if tvdb_id is None:
                tvdb_id = ''
            tmp_meta = {}
            tmp_meta['imdb_id'] = imdb_id
            tmp_meta['tvdb_id'] = tvdb_id 
            tmp_meta['title'] = name
            tmp_meta['season']  = season
            watched = self._get_watched_episode(tmp_meta)
            if watched == 6:
                self._update_watched(imdb_id, type, 7, name=name, season=season, episode=episode, tvdb_id=tvdb_id)
            else:
                self._update_watched(imdb_id, type, 6, name=name, season=season, episode=episode, tvdb_id=tvdb_id)
                
    
    def _update_watched(self, imdb_id, type, new_value, tmdb_id='', name='', season='', episode='', tvdb_id=''):
        '''
        Commits the DB update for the watched status
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'
            new_value (int): value to update overlay field with
        Kwargs:
            name (str): name of video        
            season (str): season number
            tvdb_id (str): tvdb id of tvshow                        

        '''      
        if type == self.type_movie:
            if imdb_id:
                sql="UPDATE movie_meta SET overlay = %s WHERE imdb_id = '%s'" % (new_value, imdb_id)
            elif tmdb_id:
                sql="UPDATE movie_meta SET overlay = %s WHERE tmdb_id = '%s'" % (new_value, tmdb_id)            
        elif type == self.type_tvshow:
            sql="UPDATE tvshow_meta SET overlay = %s WHERE imdb_id = '%s'" % (new_value, imdb_id)
        elif type == 'episode':
            sql="UPDATE episode_meta SET overlay = %s WHERE imdb_id = '%s' AND tvdb_id = '%s' AND season = %s AND episode = %s" % (new_value, imdb_id, tvdb_id, season, episode)
        else: # Something went really wrong
            return None
        
        try:
            print 'Updating watched status for type: %s, imdb id: %s, tmdb_id: %s, new value: %s' % (type, imdb_id, tmdb_id, new_value)
            print 'SQL UPDATE: %s' % sql
            self.dbcur.execute(sql)
            self.dbcon.commit()
        except Exception, e:
            print '************* Error attempting to update table: %s ' % e
            pass    
    
   
    def _get_watched(self, imdb_id, type):
        '''
        Finds the watched status of the video from the cache db
        
        Args:
            imdb_id (str): IMDB ID
            type (str): type of video to update, 'movie', 'tvshow' or 'episode'                    

        ''' 
        if type == self.type_movie:
            table='movie_meta'
        elif type == self.type_tvshow:
            table='tvshow_meta'
        
        try:
            self.dbcur.execute("SELECT * FROM " + table + " WHERE imdb_id = '%s'" % imdb_id)
            matchedrow = self.dbcur.fetchone()
        except Exception, e:
            print '************* Error attempting to select from %s table: %s ' % (table, e)
            pass  
                    
        if matchedrow:
            return dict(matchedrow)['overlay']
        else:
            return 6

        
    def _get_watched_episode(self, meta):
        '''
        Finds the watched status of the video from the cache db
        
        Args:
            meta (dict): full data of episode                    

        '''     
        self.dbcur.execute('SELECT * FROM episode_meta WHERE ' +
                           'imdb_id = "%s" AND tvdb_id = "%s" AND season = "%s" AND title = "%s" ' 
                           % (meta['imdb_id'], meta['tvdb_id'], meta['season'], meta['title']) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
                return dict(matchedrow)['overlay']
        else:
            return 6
    

    def _find_cover(self, season, images):
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
    

    def get_seasons(self, imdb_id, seasons):
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
                    meta['cover_url']=self._find_cover(season, images )
                else:
                    if len(season) == 4:
                        meta['cover_url']=''
                    else:
                        images = self._get_season_posters(tvdb_id, season)
                        meta['cover_url']=self._find_cover(season, images )
                        
                meta['season']=season
                meta['tvdb_id'] = tvdb_id
                meta['imdb_id'] = imdb_id
                meta['overlay'] = 6
                
                self._cache_save_season_meta(meta)
            
            coversList.append(meta)
            
        return coversList

    
    def _get_season_posters(self, tvdb_id, season):
        tvdb = TheTVDB()
        images = tvdb.get_show_image_choices(tvdb_id)       
        return images
        

    def _cache_lookup_season(self, imdb_id, season):
        '''
        Lookup data for a given season in the local cache DB.
        
        Args:
            imdb_id (str): IMDB ID
            season (str): tv show season number, number only no other characters
                        
        Returns:
            (dict) meta data for a match
        '''      
        print 'Looking up season data in cache db, imdb id: %s season: %s' % (imdb_id, season)
        self.dbcur.execute("SELECT * FROM season_meta WHERE imdb_id = '%s' AND season ='%s' " 
                           % ( imdb_id, season ) )
        matchedrow = self.dbcur.fetchone()
        if matchedrow:
            print 'Found season meta information in cache table: ', dict(matchedrow)
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
                               % ( meta['imdb_id'], meta['season'] ) ) 
            matchedrow = self.dbcur.fetchone()
            if matchedrow:
                print 'Season matched row found, deleting table entry'
                self.dbcur.execute("DELETE FROM season_meta WHERE imdb_id = '%s' AND season ='%s' " 
                                   % ( meta['imdb_id'], meta['season'] ) )
        except Exception, e:
            print '************* Error attempting to delete from cache table: %s ' % e
            print 'Meta data:', meta
            pass 
                    
        print 'Saving season cache information: ', meta
        try:
            self.dbcur.execute("INSERT INTO season_meta VALUES "
                               "(:imdb_id, :tvdb_id, :season, :cover_url, :overlay)",
                               meta
                               )
            self.dbcon.commit()
        except Exception, e:
            print '************* Error attempting to insert into cache table: %s ' % e
            print 'Meta data:', meta
            pass         
            