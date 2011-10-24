'''
create/install metadata containers,
v1.0
currently very specific to icefilms.info
'''

# NOTE: these are imported later on in the create container function:
# from cleaners import *
# import clean_dirs

import os,sys
import shutil
import xbmc,xbmcaddon

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

    def __init__(self, path='special://profile/addon_data/script.module.metahandler'):
        #!!!! This must be matched to the path in meteahandler.py MetaData __init__
        self.path = xbmc.translatePath(path)
        self.work_path = os.path.join(self.path, 'work')
     
   
    def _del_metadir(self, path=path):
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