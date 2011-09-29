'''
create/install metadata containers,
v1.0
currently very specific to icefilms.info
'''

# NOTE: these are imported later on in the create container function:
# from cleaners import *
# import clean_dirs

import re,os,sys,urllib,urllib2
import shutil
import xbmcaddon

#append lib directory
addon = xbmcaddon.Addon(id='script.module.metahandler')
path = addon.getAddonInfo('path')
sys.path.append((os.path.split(path))[0])

try: import xbmc
except:
    print 'Not running under xbmc; install container function unavaliable.'
    xbmc_imported=False
else:
    xbmc_imported=True

class MetaContainer:
    
    def GetURL(self, url):
        #print 'processing url: '+url
        req = urllib2.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-GB; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3')       
        response = urllib2.urlopen(req)
        link=response.read()
        response.close()
        return link
    
    def _updir(self, thepath, x):
            # move up x directories on thepath
            while x > 0:
                x -= 1
                thepath = (os.path.split(thepath))[0]
            return thepath
     
    def create_metadata_container(self):
    
        from cleaners import *
        import clean_dirs
    
        sys.path.append(_updir(os.getcwd(),))
        import default
    
        ####  Create a full metadata cache  ####
        # Note: please update this with the latest code from MOVIEINDEX and TVINDEX  (from default.py)
    
        #!!!! This must be matched to workdir in meteahandler.py MetaData __init__
        workdir = os.path.join(os.getcwd(),'Generated Metacontainer')
        
        print '### BUILDING CONTAINER IN:',workdir
        print ' '
    
        print '### Adding movies to database ###'
        print ' '
        print ' '
    
        #scrape A-Z of all tv shows
        default.TVA2ZDirectories('')
    
        print '### FINISHED Adding movies to database ###'
        print ' '
        print ' '
    
        print '### Adding TV Shows to database ###'
        print ' '
        print ' '
        
        #scrape A-Z of all movies
        default.MOVIEA2ZDirectories('')
    
        print '### FINISHED Adding TV Shows to database ###'
        print ' '
        print ' '
    
        print '### Cleaning image directories of empty sub-directories [Running clean_dirs.py]'
        clean_dirs.do_clean(workdir)
    
        print '### Container Making is Finished ###'
    
    def _del_metadir(self, path):
        #pass me the path the meta_caches is in
    
        meta_caches=os.path.join(path,'meta_caches')
        
        #Nuke the old meta_caches folder (if it exists) and install this meta_caches folder.
        #Will only ever delete a meta_caches folder, so is farly safe (won't delete anything it is fed)
    
        if os.path.exists(meta_caches):
                try:
                    shutil.rmtree(meta_caches)
                except:
                    print 'Failed to delete old meta'
                    return False
                else:
                    print 'deleted old meta'
                    return True
    
    def _del_path(self, path):
    
        if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                except:
                    print 'Failed to delete old meta'
                    return False
                else:
                    print 'deleted old meta'
                    return True
    
    def _extract_zip(self, src,dest):
            try:
                print 'Extracting '+str(src)+' to '+str(dest)
                #make sure there are no double slashes in paths
                src=os.path.normpath(src)
                dest=os.path.normpath(dest) 
    
                #Unzip
                xbmc.executebuiltin("XBMC.Extract("+src+","+dest+")")
    
            except:
                print 'Extraction failed!'
                return False
            else:                
                print 'Extraction success!'
                return True
     
    def install_metadata_container(self, workingdir,containerpath,dbtype,installtype):
    
        #NOTE: This function is handled by higher level functions in the Default.py
        
        if xbmc_imported==True:
    
            if dbtype=='tvshow' or dbtype=='movie':
    
                if installtype == 'database' or installtype == 'covers' or installtype == 'backdrops':
    
                    meta_caches=os.path.join(workingdir,'meta_caches')
                    imgspath=os.path.join(meta_caches,dbtype)
                    cachepath=os.path.join(meta_caches,'video_cache.db')
    
                    if not os.path.exists(meta_caches):
                        #create the meta folders if they do not exist
                        self.make_dirs(workingdir)
    
                    if installtype=='database':
                        #delete old db files
                        try: os.remove(cachepath)
                        except: pass
    
                        #extract the db zip to 'themoviedb' or 'TVDB'
                        self._extract_zip(containerpath,meta_caches)
    
                    if installtype=='covers' or installtype=='backdrops':
                        #delete old folders
                        deleted = self._del_path(os.path.join(imgspath,installtype))
    
                        #extract the covers or backdrops folder zip to 'movie' or 'tv'
                        if deleted == True: self._extract_zip(containerpath,imgspath)
                else:
                    print 'not a valid installtype:',installtype
                    return False
            else:
                print 'not a valid dbtype:',dbtype
                print 'dbtype must be either "tv" or "movie"'
                return False
        else:                          
            print 'Not running under xbmc :( install container function unavaliable.'
            return False
    
    if __name__ == "__main__":
        create_metadata_container(os.getcwd())