# Credits: Daledude, WestCoast13
# Awesome efficient lightweight code.
# last modified 19 March 2011
# added support for TVDB search for show, seasons, episodes
# also searches imdb (using http://www.imdbapi.com/) for missing info in movies or tvshows

import simplejson
import urllib, urllib2, re, socket
#from pprint import pprint

from thetvdbapi import TheTVDB            
#from string import maketrans,translate

class TMDB(object):
    def __init__(self, api_key='b91e899ce561dd19695340c3b26e0a02', view='json', lang='en'):
        #view = yaml json xml
        self.view = view
        self.lang = lang
        self.api_key = api_key
        self.url_prefix = 'http://api.themoviedb.org/2.1'

    def cleanUnicode(self, string):
        '''
        #Unfortunately, this isn't working for unicodes, but these must be all the characters that need replacing
        #Maybe i'll try to make a method similar to the translate, later
        #Also these : \xC4"=>"Ae", "\xC6"=>"AE", "\xD6"=>"Oe", "\xDC"=>"Ue", "\xDE"=>"TH", "\xDF"=>"ss", "\xE4"=>"ae", "\xE6"=>"ae", "\xF6"=>"oe", "\xFC"=>"ue", "\xFE"=>"th"
        intab = "\xA1\xAA\xBA\xBF\xC0\xC1\xC2\xC3\xC5\xC7\xC8\xC9\xCA\xCB\xCC\xCD\xCE\xCF\xD0\xD1\xD2\xD3\xD4\xD5\xD8\xD9\xDA\xDB\xDD\xE0\xE1\xE2\xE3\xE5\xE7\xE8\xE9\xEA\xEB\xEC\xED\xEE\xEF\xF0\xF1\xF2\xF3\xF4\xF5\xF8\xF9\xFA\xFB\xFD\xFF"
        outtab = "!ao?AAAAACEEEEIIIIDNOOOOOUUUYaaaaaceeeeiiiidnooooouuuyy"
        trantab = maketrans(intab, outtab)
        
        strr = string
        return translate(strr, trantab)
        '''
        try:
            string = string.replace("'","").replace(unicode(u'\xe3'), 'a').replace(unicode(u'\xe2'), 'a').replace(unicode(u'\xe0'), 'a').replace(unicode(u'\xe1'), 'a').replace(unicode(u'\xe8'), 'e').replace(unicode(u'\xc6'), 'AE').replace(unicode(u'\u2014'), '-').replace(unicode(u'\xe9'), 'e').replace(unicode(u'\u201c'), '"').replace(unicode(u'\u201d'), '"').replace(unicode(u'\u2019'),'').replace(unicode(u'\u2026'),'...').replace(unicode(u'\u2018'),'').replace(unicode(u'\u2013'),'-')
            return string
        except:
            return string  
       
    def _do_request(self, method, values):
        url = "%s/%s/%s/%s/%s/%s" % (self.url_prefix, method, self.lang, self.view, self.api_key, values)
        print url
        try:
            meta = simplejson.load(urllib.urlopen(url))[0]
        except:
            print "utter failure. probably connection issue"
            return None

        if meta == 'Nothing found.':
            return None
        else:
            return meta
        
    def _get_it(self,link, what):
        scrape=re.compile('"'+what+'":"(.+?)"').findall(link)
        if len(scrape) > 0: 
            return scrape[0]
        else:
            return None

    def _getURL(self,url):        
        req = urllib2.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-GB; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3') 
        #req.add_header('content-type','application/json; utf-8')
        response = urllib2.urlopen(req)
        link=response.read()
        response.close()
        return link   

    def _upd_key(self, meta, key):
        if meta.has_key(key) == False :
            return True 
        else:
            try:
                temp=str(self.cleanUnicode(meta[key]))
                print 'key is ' + key + ' and temp is ' + str(temp)
                if temp == '' or temp == '0.0' or temp == '0' or temp == 'None' or temp == '[]' or temp == 'No overview found.' or temp == 'TBD':
                    return True
                else:
                    return False
            except:
                return True

    def _search_imdb(self, meta, imdb_id):
        url = "http://www.imdbapi.com/?i=" + imdb_id

        try:
            link = self._getURL(url)
        # XXX don't remove any of these - all three have been encountered (in the same session)
        except (urllib2.HTTPError, urllib2.URLError, socket.error), e:
            print "Can't access %s: %s" % (url, e)
            return meta

        print 'imdb link is ->' + link
        if link == '{"Response":"Parse Error"}':
            return meta
        if self._upd_key(meta, 'overview'):
            meta['overview']=unicode(self._get_it(link, 'Plot'), 'utf-8')

        if self._upd_key(meta, 'actors'):
            meta['overview']='Starring : \n' + unicode(self._get_it(link, 'Actors'), 'utf-8') + '\n\nPlot : \n' + meta['overview']
        else:
            meta['overview']='Starring : \n' + meta['actors'] + '\n\nPlot : \n' + meta['overview']

        if self._upd_key(meta, 'posters') and self._upd_key(meta, 'imdb_poster'):
            temp=self._get_it(link, 'Poster')
            if temp != 'N/A':
                meta['imdb_poster']=temp
                print meta['imdb_poster']
        if self._upd_key(meta, 'rating'):
            temp=self._get_it(link, 'Rating')
            if temp != 'N/A' and temp !='' and temp != None:
                meta['rating']=temp
        if self._upd_key(meta, 'genres') and self._upd_key(meta, 'imdb_genres'):
            print link
            temp=self._get_it(link, 'Genre')
            if temp != 'N/A':
                meta['imdb_genres']=temp
                #print meta['imdb_poster']
        if self._upd_key(meta, 'runtime'):
            temp=self._get_it(link, 'Runtime')
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
                meta['runtime']=dur
        
        return meta

    # video_id is either tmdb or imdb id
    def getVersion(self, video_id):
        return self._do_request('Movie.getVersion', video_id)

    def getInfo(self, tmdb_id):
        return self._do_request('Movie.getInfo', tmdb_id)

    def getSeasonPosters(self, tvdb_id, season):
        tvdb = TheTVDB()
        images = tvdb.get_show_image_choices(tvdb_id)
        
        return images

    def imdbLookup(self, imdb_id, type, name):
        # Movie.imdbLookup doesn't return all the info that Movie.getInfo does like the cast.
        # So do a small lookup with getVersion just to get the tmdb id from the imdb id.
        # Then lookup by the tmdb id to get all the meta.

        meta = {}
        if type=='tvshow':
            print 'TV Show lookup'
            tvdb = TheTVDB()
            tvdb_id = tvdb.get_show_by_imdb(imdb_id)

            # if not found by imdb, try by name
            if tvdb_id == '':
                cleaned_name = name[:(len(name)-7)] # strip the trailing " (yyyy)"
                show_list=tvdb.get_matching_shows(cleaned_name)
                #print show_list
                tvdb_id=''
                prob_id=''
                for show in show_list:
                    (junk1, junk2, junk3) = show
                    #if we match imdb_id or full name (with year) then we know for sure it is the right show
                    if junk3==imdb_id or junk2==name:
                        tvdb_id=junk1
                        break
                    #if we match just the cleaned name (without year) keep the tvdb_id
                    elif junk2==cleaned_name:
                        prob_id = junk1
                if tvdb_id == '' and prob_id != '':
                    tvdb_id = prob_id

            if tvdb_id != '':
                print 'Show *** ' + name + ' *** found in TVdb. Getting details...'
                show = tvdb.get_show(tvdb_id)
                if show is not None:
                    meta['imdb_id'] = imdb_id
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
                        meta['imdb_genres'] = temp
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
                        meta = self._search_imdb( meta, imdb_id)
                    else:
                        meta['overview'] = 'Starring : \n' + meta['actors'] + '\n\nPlot : \n' + meta['overview']
                    return meta
                else:
                    meta = self._search_imdb( meta, imdb_id)
                    return meta

        try:
            tmdb_id = self.getVersion(imdb_id)['id']
        except:
            print ' Movie *** '+ imdb_id + ' *** not in tmdb. Will search imdb '
            meta = self._search_imdb( meta, imdb_id)
            return meta

        if tmdb_id:
            meta = self._do_request('Movie.getInfo', tmdb_id)

            if meta is None: # fall through to IMDB lookup
                meta = {}

            tmp_cast = []
            tmp_cast = meta.get('cast', '')
            meta['actors'] = ''
            for temp in tmp_cast:
                job=temp.get('job','')
                if job == 'Actor':
                    num=temp.get('order','')
                    if num == 0 or meta['actors'] == '':
                        meta['actors'] = temp.get('name','')
                    else:
                        meta['actors'] = meta['actors'] + ', ' + temp.get('name','')
                        if num == 4: # Read only first 5 actors, there might be a lot of them
                            break
            #print meta
            meta['actors'] = self.cleanUnicode(meta['actors'])
            # Read overview only when in English Language, to avoid some unicode errors
            if meta['language'] != 'en':
                meta['overview'] = ''
            else:
                meta['overview'] = self.cleanUnicode(meta['overview'])
            #print ' Actors ' + str(meta['actors'])
            if meta['overview'] == 'None' or meta['overview'] == '' or meta['overview'] == 'TBD' or meta['overview'] == 'No overview found.' or meta['rating'] == 0 or meta['runtime'] == 0 or str(meta['genres']) == '[]' or str(meta['posters']) == '[]' or meta['actors'] == '':
                print ' Some info missing in TMDb for Movie *** '+ imdb_id + ' ***. Will search imdb for more'
                meta = self._search_imdb( meta, imdb_id)
            else:
                meta['overview'] = 'Starring : \n' + meta['actors'] + '\n\nPlot : \n' + meta['overview']
            return meta
        else:
            meta = {}
            meta = self._search_imdb( meta, imdb_id)
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
        
        print meta
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

if __name__ == "__main__":
    print "=============="
    tmdb = TMDB('57983e31fb435df4df77afb854740ea9')
    video_meta = tmdb.imdbLookup('tt0499549')
    if not video_meta:
        raise Exception('No meta data found!')

    #pprint(video_meta)

    print "Posters:"
    for poster in video_meta['posters']:
        print "\t%s: %s" % (poster['image']['size'], poster['image']['url'])

    print "\n\n"

    print "Genres:"
    for genre in video_meta['genres']:
        print "\t%s: %s" % (genre['name'], genre['url'])

    print "\n\n"

    print "Cast:"
    for cast in video_meta['cast']:
        print "\t%s: %s" % (cast['name'], cast['job'])

    print "\n\n"

    print "Studios:"
    for studio in video_meta['studios']:
        print "\t%s" % studio['name']