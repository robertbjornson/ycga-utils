'''
Test dirs:
10x:
/ycga-gpfs/sequencers/pacbio/gw92/10x/Single_Cell/rdb9/20231007_blo8_22CNFMLT3_ma

/ycga-gpfs/sequencers/pacbio/gw92/10x/Single_Cell/blo8/20231007_blo8_22CNFMLT3_ma

/ycga-gpfs/sequencers/pacbio/gw92/10x/Single_Cell/ab2828/20230608_ab2828_H2K7LDSX7_3p

(deleted) /ycga-gpfs/sequencers/pacbio/gw92/10x/Single_Cell/db2475/20220808_db2475_5p.tar

'''
'''
Summary of operation:

Search mode
  - user provides searchdir, archivedir
  - all directories in searchdir are considered as sourcedir
  - potential archivetargets are all directories one level deep from sourcedir: searchdirdir/sourcedir/datadir.  E.g. /gpfs/ycga/sequencers/pacbio/data/r84189_20240514_154203/A1_1
  - in general, the archive files will be in archivedir/searchdirdir/sourcedir/datadir.tar.  However, by using --trimdirs, some number of leading directories from the sourcedir
    can be removed.  So, for example: when archived, we will have archive/pacbio/data/r84189_20240514_154203/A1_1.{tar,log}
  - if not otherwise specified, all dirs found under searchdir will be considered.  However, --filterdirs can be used to restrict this.  If more control is needed, use Dir/File mode.

Dir and File mode:
  - user provides on or more source dirs.  These are equivalent to the source dirs in Search mode
  - potential archive targets are one level below this dir.  We will creat archivedir/dir/data.ar


Could this be generalized a bit?

- parentdir, either tar subdirs or subsubdirs, depending on descend
- single run dir
- file of single run dirs

In all cases, the name of the tarfile corresponds to the final dir.

Search mode examples:
#1
data:/gpfs/ycga/sequencers/pacbio/data/r84189_20240514_154203/* (contains 4 subdirs)
searchdir: /gpfs/ycga/sequencers/pacbio/data
archiveDir: archive/pacbio/data
->
r84189_20240514_154203 is one of our sourcedirs, and 
we will create <ArchiverPrefix>/archive/pacbio/data/r84189_20240514_154203/A1_1.tar, etc

#2
data:/gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell/jm994/20220606_jm994_3p.tar
sourcedir: /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell
archiveDir: archive/pacbio/gw92/10x/Single_Cell
->
create archive/pacbio/gw92/10x/Single_Cell/jm994/20220606_jm994_3p.tar ...

Dir mode example using same data:
#1
sourcedir: /gpfs/ycga/sequencers/pacbio/data/r84189_20240514_154203
we will create <ArchiverPrefix>/archive/pacbio/data/r84189_20240514_154203/A1_1.tar, etc

#2
sourcedir: /gpfs/ycga/sequencers/pacbio/gw92/10x/Single_Cell/jm994
we will create archive/pacbio/gw92/10x/Single_Cell/jm994/20220606_jm994_3p.tar 

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
from CompressAndTar import compressAndTar

secPerDay=3600*24

deleteTmplt='''
This directory was deleted on %(date)s.  It is archived here: 
%(bucket)s:%(location)s
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


import os

def trim_leading_dirs(path, n):
    parts = path.split(os.sep)  # Split the path into parts based on the separator
    if path[0]=='/': n=n+1
    if n >= len(parts):         # If n is greater than or equal to the number of parts
        raise(Exception("problem with trim_leading_dirs"))
    return os.sep.join(parts[n:])

def runCmd(cmd):
    logger.debug("running %s" % cmd)
    ret=subprocess.call(cmd, shell=True)
    if (ret!=0):
        error(f"Cmd failed with {ret}")

def summarize(counter):
    logger.info("Archived %(archived)s Deleted %(deleted)d Partial %(partial)d" % counter)
 
def do(cmd):
    logger.debug(f"running cmd: {cmd}")
    ret=subprocess.run(cmd, shell=True)
    print(ret)
    if ret.returncode:
        error("cmd failed")

def archiveDS(tarBaseName, d):
    # archdir is the root of the tarfile destination
    # the new file will be archdir/rootdir.../newdir/d
    # o.staging is where we create the various files before transferring them.

    bn=os.path.basename(d)
    # local files and dirs
    tmpTarFile=f'{o.staging}/{bn}.tar'
    #logfileBN=f'{time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())}_{d}_archive.log'
    logfileBN=f'{bn}.log'
    locallogfile=f'{o.staging}/{logfileBN}'

    # src is the dir we will tar up.
    src=d

    # check date on src dir to see if ready to archive
    ts=int(os.stat(src).st_mtime)
    deltaT=time.time()-ts
    if deltaT < o.archPeriod * secPerDay:
        logger.info(f"{src} too young to archive")
        return

    remoteTarFile=f'{tarBaseName}.tar'
    remotelogfile=f'{tarBaseName}.log'
    #if "/10x/" in archiveDir: archiveDir=archiveDir.replace('ycga-gpfs/sequencers/pacbio/gw92/10x/', '10x/')
    #if "/pacbio/data/" in archiveDir: archiveDir=archiveDir.replace('ycga-gpfs/sequencers/pacbio/data', 'pacbio/data')
    

    # see which if any Archivers need this run to be archived
    if o.force:
        TodoArchivers=Archivers
    else:
        TodoArchivers=[]
        for a in Archivers:
            if a.exists(remotelogfile):
                logger.debug(f"{d} appears finished, skipping")
            elif a.exists(remoteTarFile):
                error(f"Partial archive of {d} exists")
                #runstats.errors+=1 
            else:
                TodoArchivers.append(a)

        if not TodoArchivers:
            logger.info(f'Not archiving {src}, already done.')
            return 0
        else:
            logger.debug(f'getting started {d}.  Archivers {TodoArchivers}')  


    logger.info(f'Archiving {src} to {o.bucket}:{remoteTarFile}')

    if o.fastqs:
        cmd=f"find {src} -name \"*.fastq.gz\" | tar cvf {tmpTarFile} --files-from=- > {locallogfile}" 
    else:
        cmd=f"tar cvf {tmpTarFile} {src} > {locallogfile}"

    if o.dryrun:
        logger.debug(f'dryrun: {cmd}')
    else:
        do(cmd)
        #compressAndTar(src, tmpTarFile, locallogfile, o.threads, o.staging)

    if o.encrypt:
        cmd2=f'openssl enc -aes-256-cbc -pbkdf2 -kfile ~/.ssh/ssl.key -in {tmpTarFile} -out {tmpTarFile}.enc'
        if o.dryrun:
            logger.debug(f'dryrun: {cmd2}')
        else:
            do(cmd2)
            os.remove(tmpTarFile)
        tmpTarFile=f"{tmpTarFile}.enc"
        
    # temp copies done, now move files
    for a in Archivers:
        logger.debug(f"moving {tmpTarFile} ->  {remoteTarFile}")
        logger.debug(f"moving {locallogfile} -> {remotelogfile}")
        if not o.dryrun:
            a.moveFile(tmpTarFile, remoteTarFile, extra={}) # extra={'StorageClass':'DEEP_ARCHIVE'})
            a.moveFile(locallogfile, remotelogfile) 
    # do some cleanup here
    if not o.dryrun and not o.noclean:
        os.remove(tmpTarFile)
        os.remove(locallogfile)

def deleteDS(tarBaseName, d):
    Archivers=createArchivers()

    # check date on src dir to see if ready to delete
    ts=int(os.stat(d).st_mtime)
    deltaT=time.time()-ts
    if deltaT < o.delPeriod * secPerDay:
        logger.info(f"{d} too young to delete")
        return

    remoteTarFile=f'{tarBaseName}.tar'
    remoteLogFile=f'{tarBaseName}.log'

    for a in Archivers:
        logOK=a.exists(remoteLogFile) and True
        tarOK=a.exists(remoteTarFile) and True
        logger.debug(f"logOK {logOK}, tarOK {tarOK}")
        if not (logOK and tarOK):
            logger.error(f"expected archive {remoteTarFile} invalid, not deleting dataset")
            return

    cmd=f"rm -rf {d}"
    msg=deleteTmplt % {'date':(time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())), 'bucket':o.bucket, 'location':remoteTarFile}
    logger.info(f"Deleting {d}")
    if not (o.dryrun or o.nodel):
        runCmd(cmd)
        deletedFName=f"{d}.deleted"
        open(deletedFName, "w").write(msg)

def createArchivers():
    global Archivers
    # create archivers
    neseTape='23aa87a8-8c58-418d-8326-206962d9e895'
    mccleary='ad28f8d7-33ba-4402-804e-3f454aeea842'
    mcc_endpoint='fa56d2d4-adfd-4f1e-b735-5f28bde144d7'

    mcc_test_target='924c6f20-aa6f-41ef-bfdf-ada650163378'
    # maps ~/palmer_scratch/TestTarget/

    #Archivers=[S3Interface.client(logger, bucket='ycgatestbucket', credentials='/home/rdb9/.aws/credentials', profile='default')]
    Archivers=[S3Interface.client(logger, bucket=o.bucket, credentials='/home/rdb9/.aws/credentials', profile='default')]
    #Archivers=[S3Interface.client(logger, bucket='ycgasequencearchive', credentials='/home/rdb9/.aws/credentials', profile='default')]


    #Archivers=[S3Interface.client(logger), GlobusInterface.client(logger, mccleary, mcc_test_target),]
    #Archivers=[S3Interface.client(logger), GlobusInterFace.client(logger, mcc_endpoint, mcc_test_target)]
    #Archivers=[GlobusInterface.client(logger, mccleary, neseTape), S3Interface.client(logger)]
    for a in Archivers:
        logger.debug(f"Archiver: {a}")
    return Archivers

def genTarBaseName(fsd):
    tmp=trim_leading_dirs(fsd, o.trimdirs)
    return(os.path.join(o.archDir, tmp))
           
if __name__=='__main__':
    parser=argparse.ArgumentParser(epilog=info, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--searchdir", dest="searchdir", help="archive all directories found in this directory")
    parser.add_argument("--dir", dest="dir", help="archive this directory")
    parser.add_argument("--file", dest="file", help="file containing directories to archive, using same logic as --dir")
    parser.add_argument("--archDir", dest="archDir", default="archive", help="archive path.  This will replace searchdir in archive file")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("--fastqs", dest="fastqs", action="store_true", default=False, help="only tar *.fastq.gz files")
    parser.add_argument("--archPeriod", dest="archPeriod", type=int, default=30, help="waiting period for archiving")
    parser.add_argument("--delPeriod", dest="delPeriod", type=int, default=365, help="waiting period for deletion")
    parser.add_argument("--nodel", dest="nodel", action="store_true", default=False, help="skip actual deletion, but do everything else")
    parser.add_argument("--noarch", dest="noarch", action="store_true", default=False, help="skip actual archiving, but do everything else")
    parser.add_argument("--noclean", dest="noclean", action="store_true", default=False, help="don't remove staged files")
    parser.add_argument("-l", "--logfile", dest="logfile", default="arch_del", help="logfile prefix")
    parser.add_argument("--staging", dest="staging", default=tempfile.mkdtemp(prefix='/home/rdb9/palmer_scratch/staging/'), help="staging prefix of dir for tars and log file")
    parser.add_argument("--nodescend", dest="descend", action="store_false", default=True, help="10x run dirs can have netid super-directories.  Descend one level")
    parser.add_argument("--encrypt", dest="encrypt", action="store_true", default=False, help="encrypt files")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force to overwrite tar or finished files")
    parser.add_argument("--trimdirs", dest="trimdirs", type=int, default=0, help="number of leading dirs to trim from root when naming tarball")
    # current trims: 10x: 4, pacbio: 2 
    parser.add_argument("--workers", dest="workers", type=int, default=1, help="number of workers in pool")
    parser.add_argument("--threads", dest="threads", type=int, default=4, help="number of threads for Spring")
    parser.add_argument("--maxruns", dest="maxruns", default=0, type=int, help="Only archive this many runs (for testing purposes)")
    parser.add_argument("--bucket", dest="bucket", default="ycgasequencearchive", help="bucket to archive to")
    #parser.add_argument("--filterdirs", dest="filterdirs", default=None, help="Only archive source dirs that match the filter")
        
    # TODO need staging dir
    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(filename)s:%(lineno)s %(process)s %(levelname)s %(message)s")
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

    #pool=multiprocessing.Pool(o.workers, initializer=createArchivers)

    # sanity check some arguments
    if sum(1 for x in [o.searchdir, o.dir, o.file] if x is not None) != 1:
        error("exactly one of --searchdir, --dir, --file must be provided")

    if o.searchdir:
        assert(os.path.isdir(o.searchdir))
    if o.dir:
        assert(os.path.isdir(o.dir))
    if o.file:
        assert(os.path.isfile(o.file))

    counter={"deleted":0, "archived":0, "partial":0}
    now=time.time()
    targets=[]

    """ collect all targets to be examined into a list.  Targets are the actual directories that will become
    tarballs.  If descend is set, descend one level to the actual targets.  Targets contains just the dir or a dir/subdir
    targets list elements are: [archdir, (full) grand parent dir, parent dir (optional, can be None), dir]
    """
    if o.searchdir:
        for d in sorted(os.listdir(o.searchdir)):
            fd=os.path.join(o.searchdir, d)
            if not os.path.isdir(fd): continue
            if o.descend:
                for sd in sorted(os.listdir(fd)):
                    fsd=os.path.join(fd, sd)
                    if not os.path.isdir(fsd): continue
                    tbn=genTarBaseName(fsd)
                    targets.append((tbn, fsd))
            else:
                tbn=genTarBaseName(fd)
                targets.append((tbn, fd))
    elif o.file:
        for fd in open(o.file):
            if not os.path.isdir(fd):
                error("--dir must be a directory")
            tbn=genTarBaseName(fd)
            targets.append((tbn, fd))
    elif o.dir:
        fd = o.dir
        if not os.path.isdir(fd):
            error("--dir must be a directory")
        tbn=genTarBaseName(fd)
        targets.append((tbn, fd))
    else:
        error("Should never get here")

    tmpArchivers=createArchivers()
    # First archive whatever needs archiving, depending on date and archive status
    # consider parallelizing here
    logger.info("Archive Phase")

    '''
    if o.filterdirs:
        pat=re.compile(o.filterdirs)
        targets=[t for t in targets if pat.match(
    '''
    
    if o.maxruns:
        targets=targets[:o.maxruns]
        
    if not o.noarch:
        for t in targets:
            archiveDS(*t)
    #pool.starmap(archiveDS, targets)
    
    # Next delete anything that is old enough to be deleted, and for which valid archives exist in all Archivers.

    logger.info("Delete Phase")
    if not o.nodel:
        for t in targets:
            ok=deleteDS(*t)
    
    
