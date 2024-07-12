
'''
Summary of operation:
  - user provides sourcedir, archivedir
  - potential archivetargets are all directories two levels deep from sourcedir, e.g. sourcedir/d1/d2
  - when archived, we'll have archivedir/d1/d2.tar
  - if not otherwise specified, all such archivetargets found under sourcedir will be considered, unless user provides a single target or a file of targets
  - such a specified target can be d1 or d1/d2


Could this be generalized a bit?

- parentdir, either tar subdirs or subsubdirs, depending on descend
- single run dir
- file of single run dirs

In all cases, the name of the tarfile corresponds to the final dir.

Examples:
#1
searchdir: /gpfs/ycga/sequencers/pacbio/data/
srcdir: /gpfs/ycga/sequencers/pacbio/data/r84189_20240514_154203 (contains 4 subdirs)
archiveDir: archive/pacbio/data/
->
we will create <ArchiverPrefix>/archive/pacbio/data/r84189_20240514_154203/A1_1.tar, etc

#2
searchdir: /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell
srcdir: /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell/jm994
archiveDir: archive/pacbio/gw92/10x/Single_Cell/jm994/

create archive/pacbio/gw92/10x/Single_Cell/jm994/20220606_jm994_3p.tar ...


 
'''

'''
Todo: 
  - add deletion logic?
  - remove staging files as soon as not needed.
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
    - directory containing runs?
    - single run
    - file containing runs

  - arch period, before which which don't archive yet.  This might only apply to directory method
  - del period, before which which don't delete yet.  This might only apply to directory method

  - only archive some file types?  fastqs?
  - maybe not encrypt version that goes to AWS?
  - try encrypting each read file separately, since that would be much more conveneient for users?  
     Verified that it works, uncompressing provides identical file to original and to encryption by pair.  The two individual files are a bit
     larger (2.0+2.2GB versus 3.5GB for pair).  
  - use Dask to parallelize the loop
'''

'''
how to decrypt files
time openssl aes-256-cbc -d -pbkdf2 -kfile ~/.ssh/ssl.key -in run4.tar.enc -out run4.tar
'''

import argparse, os, datetime, time, logging, subprocess, sys, string, tempfile
import S3Interface, GlobusInterface
import multiprocessing

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


def archiveDS(archdir, rootdir, newdir, d):

    # o.staging is where we create the various files before transferring them.
    # archdir is the destination

    # local files and dirs

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
        archiveDir=f'{archdir}/{newdir}/{d}'
    else:
        archiveDir=f'{archdir}/{d}'
    if o.encrypt:
        remoteTarFile=f'{archiveDir}/{d}.tar.enc'
    else:
        remoteTarFile=f'{archiveDir}/{d}.tar'
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

    fltr=("-name \"*.fastq.gz\"" if o.fastqs else "")
    cmd1=f'find {src} {fltr} | tar -cvpf {tmpTarFile} --files-from=- | xargs -I XXX sh -c "test -f XXX && md5sum XXX" > {locallogfile}; exit 0'
    if o.dryrun:
        logger.info(f'dryrun: {cmd1}')
    else:
        do(cmd1)
    if o.encrypt:
        cmd2=f'openssl enc -aes-256-cbc -pbkdf2 -kfile ~/.ssh/ssl.key -in {tmpTarFile} -out {tmpTarFile}.enc'
        if o.dryrun:
            logger.info(f'dryrun: {cmd2}')
        else:
            do(cmd2)
            os.remove(tmpTarFile)
        tmpTarFile=f"{tmpTarFile}.enc"
        
    # prep done, now move files
    for a in Archivers:
        if o.dryrun:
            logger.info(f"dryrun: moving tarfile, log file, and finished file")
        else:
            a.moveFile(tmpTarFile, remoteTarFile, extra={}) # extra={'StorageClass':'DEEP_ARCHIVE'})
            a.moveFile(locallogfile, remotelogfile) 
            a.moveFile(dummy.name, finished)
    # do some cleanup here
    os.remove(tmpTarFile)
    os.remove(locallogfile)

def deleteDS(run):
    Archivers=createArchivers()
    archdir, rootdir, newdir, d = run
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
    remoteTarFile=f'{archiveDir}/{d}.tar'
    remoteTarFileEnc=f'{archiveDir}/{d}.tar.enc'
    for a in Archivers:
        if not (a.exists(finished) and (a.exists(remoteTarFile) or a.exists(remoteTarFileEnc))):
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

def createArchivers():
    global Archivers
    # create archivers
    neseTape='23aa87a8-8c58-418d-8326-206962d9e895'
    mccleary='ad28f8d7-33ba-4402-804e-3f454aeea842'
    mcc_endpoint='fa56d2d4-adfd-4f1e-b735-5f28bde144d7'
    mcc_test_target='924c6f20-aa6f-41ef-bfdf-ada650163378'
    
    Archivers=[S3Interface.client(logger), GlobusInterface.client(logger, mccleary, mcc_test_target),]
    #Archivers=[S3Interface.client(logger), GlobusInterFace.client(logger, mcc_endpoint, mcc_test_target)]
    #Archivers=[GlobusInterface.client(logger, mccleary, neseTape), S3Interface.client(logger)]
    for a in Archivers:
        logger.debug(f"Archiver: {a}")
    return Archivers

if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=info, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--searchdir", dest="searchdir", help="archive all directories found in this directory")
    parser.add_argument("--dir", dest="dir", help="archive this directory")
    parser.add_argument("--file", dest="file", help="file containing directories to archive")
    parser.add_argument("--archDir", dest="archDir", help="archive path")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("--fastqs", dest="fastqs", action="store_true", default=False, help="only tar *.fastq.gz files")
    parser.add_argument("--archPeriod", dest="archPeriod", type=int, default=30, help="waiting period for archiving")
    parser.add_argument("--delPeriod", dest="delPeriod", type=int, default=365, help="waiting period for deletion")
    parser.add_argument("--nodel", dest="nodel", action="store_true", default=False, help="skip actual deletion, but do everything else")
    parser.add_argument("-l", "--logfile", dest="logfile", default="arch_del", help="logfile prefix")
    parser.add_argument("--staging", dest="staging", default=tempfile.mkdtemp(prefix='/home/rdb9/palmer_scratch/staging/'), help="staging prefix of dir for tars and log file")
    parser.add_argument("--nodescend", dest="descend", action="store_false", default=True, help="10x run dirs can have netid super-directories.  Descend one level")
    parser.add_argument("--encrypt", dest="encrypt", action="store_true", default=False, help="encrypt files")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force to overwrite tar or finished files")
    parser.add_argument("-t", "--workers", dest="workers", type=int, default=1, help="number of workers in pool")
    # TODO need staging dir
    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(filename)s %(process)d %(lineno)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    logfname=f'{o.staging}/{time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime())}_{o.logfile}'
    h=logging.FileHandler(logfname)
    h.setLevel(logging.DEBUG)
    h.setFormatter(formatter)
    logger.addHandler(h)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    logger.info(o)

    pool=multiprocessing.Pool(o.workers, initializer=createArchivers)

    if sum(1 for x in [o.archDir, o.file] if x is not None) != 1:
        error("exactly one of --archDir --file must be provided")

    if sum(1 for x in [o.searchdir, o.dir, o.file] if x is not None) != 1:
        error("exactly one of --searchdir, --dir, --file must be provided")

    counter={"deleted":0, "archived":0, "partial":0}

    if o.searchdir:
        assert(os.path.isdir(o.searchdir))
    if o.dir:
        assert(os.path.isdir(o.dir))
    if o.file:
        assert(os.path.isfile(o.file))
        
    now=time.time()

    runs=[]


    """ collect all runs to be examined into a list.  If descend is set, if directory looks like
    a netid, descend one level to the actual runs.  Runs contains just the dir or a dir/subdir
    """
    if o.searchdir:
        for d in sorted(os.listdir(o.searchdir)):
            fd=os.path.join(o.searchdir, d)
            if not os.path.isdir(fd): continue
            if o.descend:
                for sd in sorted(os.listdir(fd)):
                    fsd=os.path.join(fd, sd)
                    if not os.path.isdir(fsd): continue
                    runs.append([o.archDir, o.searchdir, d, sd])
            else:
                runs.append([o.archDir, o.searchdir, None, d])
    elif o.file:
        for entry in open(o.file):
            dir, archdir=entry.split()
            pref, base=os.path.split(dir)
            runs.append([archdir, pref, None, base])
    elif o.dir:
        pref, base=os.path.split(o.dir)
        runs.append([o.archDir, pref, None, base])
    else:
        error("Should never get here")

    tmpArchivers=createArchivers()
    # First archive whatever needs archiving, depending on date and archive status
    # consider parallelizing here
    logger.debug("Archive Phase")
    pool.starmap(archiveDS, runs)
    
    # Next delete anything that is old enough to be deleted, and for which valid archives exist in all Archivers.
    if not o.nodel:
        logger.debug("Delete Phase")
        for r in runs:
            ok=deleteDS(r)

    
    
