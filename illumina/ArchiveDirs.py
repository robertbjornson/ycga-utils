
'''
Surrmy of poperation:
  - user provides sourcedir, archivedir
  - potential archivetargets are all directories two levels deep from sourcedir, e.g. sourcedir/d1/d2
  - when archived, we'll have archivedir/d1/d2.tar
  - if not otherwise specified, all such archivetargets found under sourcedir will be considered, unless user provides a single target or a file of targets
  - such a specified target can be d1 or d1/d2

'''

'''
Todo: 
  - add deletion logic?
  - add ability to provide list of directories to archive?
  - think about how to represent target directories in requests.  Need to privide path to directory, but also what prefix to delete.  E.g. for single cell, 
          replace /gpfs/ycga/sequencers with archive/
     The old script expected full paths for src and archive dirs.  subdirs of src were appended to archive dir.  
     /gpfs/ycga/sequencers/pacbio/data/<run> (r84189_20240514_154203)
        current: /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/data/r84189_20240514_154203  
          1_A01.finished
          1_A01.tar

     /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell/<netid>/<run>
        current: /SAY/archive/YCGA-729009-YCGA-A2/archive/pacbio/gw92/10x/Single_Cell/yta4
          20220418_yta4_5cite.finished
          20220418_yta4_5cite.tar

  - ways to specify runs to examine:
    - directory
    - single run
    - file containing runs

  - arch period, before which which don't archive yet.  This might only apply to directory method
  - del period, before which which don't delete yet.  This might only apply to directory method

  - only archive some file types?  fastqs?

'''


import argparse, os, datetime, time, logging, subprocess, sys, string, tempfile
import S3Interface

secPerDay=3600*24

deleteTmplt='''
This directory was deleted on %(date)s.  It is archived here: 
%(location)s
'''

info='''This script takes a directory and examines each top level subdirectory.  
If the mtime is older than archPeriod, the directory will be tarred up to archDir.
A *.finished file will also be created in the archive to indicate completion.
If the mtime is older than delPeriod, AND the directory has been archived, it will be deleted.
A *.deleted file will be left behind containing the location of the archive tarball.
Presumably it will be run as root to be able to do this.
'''

''' Utility error function.  Bomb out with an error message and code '''
def error(msg):
    logger.error(msg)
    raise RuntimeError(msg)


def runCmd(cmd):
    logger.debug("running %s" % cmd)
    ret=subprocess.call(cmd, shell=True)
    if (ret!=0):
        logger.error("Cmd failed with %d" % ret)
        sys.exit(ret)

def summarize(counter):
    logger.info("Archived %(archived)s Deleted %(deleted)d Partial %(partial)d" % counter)
 
def do(cmd):
    logger.debug(f"running cmd: {cmd}")
    ret=subprocess.run(cmd, shell=True)
    print(ret)
    if ret.returncode:
        error("cmd failed")


def archiveDS(Archivers, o, newdir, d):
    # o.staging is where we create the various files before transferring them.
    # archiveDir is the destination

    # local files and dirs

    rootdir=o.dir
    tmpTarFile=f'{o.staging}/{d}.tar'
    logfileBN=f'{time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())}_{d}_archive.log'
    locallogfile=f'{o.staging}/{logfileBN}'
    dummy=tempfile.NamedTemporaryFile(dir=o.staging)

    if newdir:
        src=f'{rootdir}/{newdir}/{d}'
    else:
        src=f'{rootdir}/{d}'

    # check date on src dir to see if ready to archive
    ts=int(os.stat(src).st_mtime)
    deltaT=time.time()-ts
    if deltaT < o.archPeriod * secPerDay:
        logger.debug(f"{src} too young to archive")
        return

    #remote files and dirs
    if newdir:
        archiveDir=f'{o.archDir}/{newdir}/{d}'
    else:
        archiveDir=f'{o.archDir}/{d}'
    remoteTarFile=f'{archiveDir}/{d}.tar.enc'
    finished=f'{archiveDir}/finished.txt'
    remotelogfile=f'{archiveDir}/{logfileBN}'

    # see which if any Archivers need this run to be archived
    if o.force:
        TodoArchivers=Archivers
    else:
        TodoArchivers=[]
        for a in Archivers:
            if a.exists(finished):
                logger.debug(f"{d} appears finished, skipping")
            elif a.exists(remoteTarFile):
                logger.error(f"Partial archive of {d} exists")
                #runstats.errors+=1 
            else:
                TodoArchivers.append(a)

        if not TodoArchivers:
            logger.debug('nothing to do for this run')
            return 0
        else:
            logger.debug(f'getting started {d}.  Archivers {TodoArchivers}')  

    cmd1=f'tar -cvpf {tmpTarFile} {src} | xargs -I XXX sh -c "test -f XXX && md5sum XXX" > {locallogfile}; exit 0'
    if o.dryrun:
        logger.info(f'dryrun: {cmd1}')
    else:
        do(cmd1)
    cmd2=f'openssl enc -aes-256-cbc -pbkdf2 -kfile ~/.ssh/ssl.key -in {tmpTarFile} -out {tmpTarFile}.enc'
    if o.dryrun:
        logger.info(f'dryrun: {cmd2}')
    else:
        do(cmd2)

    # prep done, now move files
    for a in Archivers:
        if o.dryrun:
            logger.info(f"dryrun: moving tarfile, log file, and finished file")
        else:
            a.moveFile(f'{tmpTarFile}.enc', remoteTarFile, extra={'StorageClass':'DEEP_ARCHIVE'})
            a.moveFile(locallogfile, remotelogfile) 
            a.moveFile(dummy.name, finished)

def deleteDS(Archivers, o, newdir, d):
    rootdir=o.dir
    if newdir:
        src=f'{rootdir}/{newdir}/{d}'
    else:
        src=f'{rootdir}/{d}'

    # check date on src dir to see if ready to delete
    ts=int(os.stat(src).st_mtime)
    deltaT=time.time()-ts
    if deltaT < o.delPeriod * secPerDay:
        logger.debug(f"{src} too young to delete")
        return

    # now check that valid archive exists on each Archiver
    if newdir:
        archiveDir=f'{o.archDir}/{newdir}/{d}'
    else:
        archiveDir=f'{o.archDir}/{d}'
    finished=f'{archiveDir}/finished.txt'
    remoteTarFile=f'{archiveDir}/{d}.tar.enc'
    for a in Archivers:
        if not (a.exists(finished) and a.exists(remoteTarFile)):
            logger.error(f"expected archive {remoteTarFile} invalid, not deleting dataset")
            return

    cmd=f"rm -rf {src}"            
    msg=deleteTmplt % {'date':(time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())), 'location':remoteTarFile}
    if o.dryrun:
        logger.info(cmd)
    else:
        logger.debug(cmd)
        runCmd(cmd)
        deletedFName=src+".deleted"
        open(deletedFName, "w").write(msg)
    
    
if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=info, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir")
    parser.add_argument("archDir")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("--fastqs", dest="fastqs", action="store_true", default=False, help="only tar *.fastq.gz files")
    parser.add_argument("--archPeriod", dest="archPeriod", type=int, default=30, help="waiting period for archiving")
    parser.add_argument("--delPeriod", dest="delPeriod", type=int, default=365, help="waiting period for deletion")
    parser.add_argument("--nodel", dest="nodel", action="store_true", default=False, help="skip actual deletion, but do everything else")
    parser.add_argument("-l", "--logfile", dest="logfile", default="arch_del", help="logfile prefix")
    parser.add_argument("--staging", dest="staging", default=tempfile.mkdtemp(prefix='/home/rdb9/palmer_scratch/staging/'), help="staging prefix of dir for tars and log file")
    parser.add_argument("--descend", dest="descend", action="store_true", default=False, help="10x run dirs can have netid super-directories.  Descend one level")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force to overwrite tar or finished files")
    # TODO need staging dir
    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(filename)s %(lineno)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    logfname=f'{o.staging}/{time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())}_{o.logfile}'
    h=logging.FileHandler(logfname)
    h.setLevel(logging.DEBUG)
    h.setFormatter(formatter)
    logger.addHandler(h)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    logger.info(o)

    counter={"deleted":0, "archived":0, "partial":0}

    assert(os.path.isdir(o.dir))
    now=time.time()

    runs=[]

    # create archivers
    neseTape='23aa87a8-8c58-418d-8326-206962d9e895'
    mccleary='ad28f8d7-33ba-4402-804e-3f454aeea842'
    Archivers=[S3Interface.client(logger), ]
    #Archivers=[GlobusInterface.client(logger, mccleary, neseTape), S3Interface.client(logger)]

    """ collect all runs to be examined into a list.  If descend is set, if directory looks like
    a netid, descend one level to the actual runs.  Runs contains just the dir or a dir/subdir
    """
    for d in sorted(os.listdir(o.dir)):
        fd=os.path.join(o.dir, d)
        if not os.path.isdir(fd): continue
        if o.descend and d[0] in string.ascii_letters:
            for sd in sorted(os.listdir(fd)):
                fsd=os.path.join(fd, sd)
                if not os.path.isdir(fsd): continue
                runs.append([d, sd])
        else:
            runs.append([None, d])

    # First archive whatever needs archiving, depending on date and archive status 
    for newdir, d in runs:
        ok=archiveDS(Archivers, o, newdir, d)

    # Next delete anything that is old enough to be deleted, and for which valid archives exist in all Archivers.
    if not o.nodel:
        for newdir, d in runs:
            ok=deleteDS(Archivers, o, newdir, d)

    
    
