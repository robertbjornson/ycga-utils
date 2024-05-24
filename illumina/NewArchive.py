'''
Example run:
/gpfs/ycga/sequencers/illumina/sequencerB/runs221109_M01156_0786_000000000-KDHKH/Data/Intensities/BaseCalls/Unaligned
'''

''' changes required to use S3 instead of filesystem

use cli or python api to aws??

os.path.exists(pth) -> check bucket
os.mkdirs(dir) -> ?
tarfile already exists?
finished file exists?
move of tarball to archive
touch the finished file

validate: really download and validate?  Or validate the staged copy?

allow storageclass intelligent_tiering

'''

'''
This script examines a number of illumina runs, and attempts to archive selected runs to another
directory as a set of tarballs.  One tarball is made for each "project", and one main tarball
is made for everything else.

Todo
- DONE avoid clobbering project tars if multiple projects of same name exist in tree
- LATER change owner of project tars to pi and pi group, chmod to 440

'''

'''
Data is either pre 1.8 or 1.8.

Pre 1.8:  
  unmapped fastq data is in files named: s_1..export.txt[.gz]
  mapped export format data is in files named: s_1..sequence.txt[.gz]

Both are not always present, since deletion of sequence files was optional, as was mapping.  
This script omits exports if sequence exists.  If no sequence, exports will be saved as is, rather than converted back.

1.8:
  unmapped fastq data is in files named like this: SAG101-1P_GCCAAT-GCCAAT_L006_R2_003.fastq.gz
  they should always be present, by convention only, in Unaligned*/Project_*/Sample_*/

  mapped files may not be present, and are named like this: SAG101-1P_GCCAAT-GCCAAT_L006_R2_003.export.txt.gz
  if present, they are by convention only, in Unaligned*/Project_*/Sample_*/

For pre1.8 runs, we'll aim to make a single tarball with the important small files, plus the fastq files as quip files, and only exports for which there were no
fastqs found.

For 1.8 runs, we'll aim to make a main tarball with important small files.  Then, for each Unaligned*/Project* directory, we'll make a separate tarball of that, so that
data belonging to different users is stored in separate tarballs.

To do a single archive run:

python archive.py [-n] -v -r /ycga-gpfs/sequencers/illumina/sequencerY/runs/161007_K00162_0117_AHFT32BBXX

'''

import os, tarfile, subprocess, logging, argparse, sys, re, tempfile, time, threading, hashlib, gzip, glob, datetime, shutil

#import S3Interface, CS
import GlobusInterface

# Directories matching these patterns, and everything below them, will not be archived
ignoredirs=r'''Aligned\\S*$
oDiag$
Calibration\S*$
EventScripts$
Images$
InterOp$
Logs$
PeriodicSaveRates$
Processed$
Queued$
ReadPrep1$
ReadPrep2$
Recipe$
Temp$
Thumbnail_Images$
DataRTALogs$
Data/TileStatus$
Data/Intensities/L\d*$
Data/Intensities/Offsets$
Data/Intensities/BaseCalls/L\d*$
GERALD\S*/CASAVA\S*$
GERALD\S*/Temp$
Matrix$
Phasing$
Plots$
Stats$
SignalMeans$
L00\d$
'''
ignoreDirsPat=re.compile('|'.join(ignoredirs.split()))

# Files matching these patterns will not be archived
ignorefiles='''s_+.+_anomaly.txt$
s_+.+_reanomraw.txt$
'''
ignoreFilesPat=re.compile(r'|'.join(ignorefiles.split()))

# Files matching these patterns are unmapped reads, and will be quipped and archived
fastqfiles=r'''\.fastq\.txt\.gz$
\.fastq$
\.fastq.gz$
\.fq$
\_sequence.txt.gz
'''
fastqFilesPat=re.compile(r'|'.join(fastqfiles.split()))

# Files matching these patterns are old style mapped reads, and will be archived only if matching unmapped reads are not present
oldexportfiles=r'''s_\d_.*export.txt.gz$
s_\d_.*export.txt$
'''
oldexportFilesPat=re.compile(r'|'.join(oldexportfiles.split()))

# quip files will be archived as is
quipfiles='''.qp$'''
quipFilesPat=re.compile(r'|'.join(quipfiles.split()))

# Directories matching this pattern are 1.8 Unaligned Project directories.  We want to archive their contents to a project-specific tar file.
projectPat=re.compile(r'(Unaligned[^/]*)/Project_[\w\-_]+$')

epilog="epilog"

'''
This function strips off any part of the path that comes before the standard path 
beginning.  For example, '/gpfs/scratch/ycga/data/panfs..' will be reduced to '/panfs..'

If such a prefix isn't found, we just use the path as given to us
'''

''' generate archive path from original path
'''
def mkarcdir(pth, archivetop):
    i=pth.find('/panfs/sequencers')
    if i != -1: return archivetop+pth[i:]
    i=pth.find('/ycga-ba/ba_sequencers')
    if i != -1: return archivetop+pth[i:]
    i=pth.find('/ycga-gpfs/sequencers/illumina')
    if i != -1: return archivetop+pth[i:]

    if pth.startswith('/'):
        return archivetop+pth
    else:
        return archivetop+'/'+pth

'''
This class wraps a tarfile object.  It adds a couple of bits of functionality:
It keeps track of what's been added and refuses to add the same archive name twice
It keeps a list of validation tasks to be done at the end
'''
class tarwrapper(object):
    def __init__(self, fn):
        self.fn=fn # name of tar file
        self.tmpfn=o.staging+'/'+os.path.basename(fn)
        self.tfp=tarfile.open(self.tmpfn, 'w')
        #self.tfp=tarfile.open(fn, 'w')
        self.added=set() #files already added to archive
        self.check=[] # validation task to run at the end

    ''' 
    name is the actual file holding the data (sometimes a temp file)
    origname is the original file name (full path)
    arcname is the name we want on the archive (full path)
    '''
    def add(self, name, origname=None, arcname=None):
        if not origname:
            origname=name
        if not arcname:
            arcname=name
        if arcname in self.added:
            error("Attempting to overwrite %s in %s" % (arcname, self.fn))
        logger.debug("Adding %s" % name)
        self.tfp.add(name, arcname)
        self.added.add(arcname)
        self.check.append(validatejob(origname, arcname, o.testlen))

    def finalize(self):
        self.tfp.close()
        logger.debug("Closing %s" % self.tmpfn)
        logger.debug("Moving %s to %s" % (self.tmpfn, self.fn))
        Archiver.moveFile(self.tmpfn, self.fn) 

    def validate(self):
        return True  ## FIX
        self.tfp.close()
        objhead=Archiver.getHead(self.fn)
        logger.debug(f'Validating {self.fn}')
        ret = CS.compare(self.tmpfn, objhead)
        if not ret:
            error("Validation failed")
        return ret

''' utility function '''
def countTrue(*vals):
    cnt=0
    for v in vals:
        if v: cnt+=1
    return cnt

''' This function turns a negative date delta into a 6 digit date that many days in the past.  Anything else is returned unchanged '''
def fixCut(cut):
    try:
        if int(cut) < 0:
            d=datetime.datetime.now() + datetime.timedelta(days=int(cut))
            return d.strftime("%y%m%d")
    except:
        pass
    return cut

''' Utility error function.  Bomb out with an error message and code '''
def error(msg):
    logger.error(msg)
    raise RuntimeError(msg)

'''
Prune the search by removing any directory that matches ignoresPat from the dirs list.
Remove and return any directories that look like: "Unaligned*/Project_*".  We'll handle them separately.
'''
def prunedirs(dirname, dirs):
    projects=[]
    for d in dirs[:]: # use a copy of the list
        dn=dirname+'/'+d
        if ignoreDirsPat.search(dn): 
            logger.debug("pruning %s" % dn)
            dirs.remove(d)
            continue
        if o.projecttars:
            mo=projectPat.search(dn)
            if mo:
                logger.debug("found project %s" % dn)
                projects.append((d, mo.group(1))) # capture the Unaligned* dir name
                dirs.remove(d)
    return projects

def prunefiles(dirname, files):
    for f in files[:]: # use a copy of the list 
        fn=dirname+'/'+f
        if ignoreFilesPat.search(f): 
            logger.debug("skipping %s" % fn)
            files.remove(f)
    return files


''' utility class to hold various counts'''
class stats(object):
    def __init__(self):
        self.bytes=0
        self.quips=0
        self.files=0
        self.tarfiles=0
        self.runs=0
        
    def comb(self, other):
        self.bytes+=other.bytes
        self.quips+=other.quips
        self.files+=other.files
        self.tarfiles+=other.tarfiles
        self.runs+=other.runs

''' encapsulates a validation task.  We may parallelize these similar to quipjobs in the future 
Most files are validated by simply comparing the first portion of the file from both the archive and the original.
quipped files are different: The quip file must be dequipped, and the original (usually gzipped) is uncompressed before comparing
We usually only do the first 10000 bytes to save time.
'''
class validatejob(object):
    def __init__(self, fn, tn, testlen=10000):
        self.fn=fn #original name
        self.tn=tn # archive name
        self.testlen=testlen # number of bytes to compare
        self.status=False

    def __str__(self):
        return "Validate Job "+self.fn

'''
This function is called to create the main tarball for a run, and also called recursively to archive each Unaligned/Project.
top: the directory to archive
arcdir: directory in which to create the tarball 
name: name of tarball in that directory

We'll also keep track of the files and bytes archived as we go.
Finally, to avoid a collision if there are multiple Project dirs with the same name, we are adding a counter to the tarball name.  
We will also create a log file and a finished file.  Thus:

141215_M01156_0172_000000000-AAR3L/
  141215_M01156_0172_000000000-AAR3L_0.tar
  141215_M01156_0172_000000000-AAR3L_1_Project_Ccc7.tar
  141215_M01156_0172_000000000-AAR3L_2_Project_Rdb9.tar
  141215_M01156_0172_000000000-AAR3L_archive.log
  141215_M01156_0172_000000000-AAR3L_finished.txt

'''
def makeTarball(top, arcdir, name, runstats):

    tfname="%s/%s.tar" % (arcdir, name % runstats.tarfiles)
    if Archiver.exists(tfname) and not o.force: error("%s exists, use -f to force" % tfname)
    logger.debug("creating tarfile %s" % tfname) 
    if o.dryrun:
        tfp=None
    else:
        tfp=tarwrapper(tfname)

    filesToCheck=[]
    fastqs=[]
    for dirname, dirs, files in os.walk(top):
        files.sort()
        # prunedirs does two things:
        # 1)removes unwanted directorys from the list
        # 2)removes and return a list of dirs that look like projects, which will be handled in a separate tarballs
        projects=prunedirs(dirname, dirs)
        for proj, unalignedDir in projects:
            makeTarball(dirname+'/'+proj, arcdir, "%s_%%s_%s_%s" % (o.runname, unalignedDir, proj), runstats)

        files=prunefiles(dirname, files)        
        # add the remaining keepers
        for f in files:
            fp=dirname+'/'+f
            try:
                sz=os.stat(fp).st_size
                logger.debug("adding %s (%d bytes)" % (fp, sz))
                runstats.files+=1; runstats.bytes+=sz
            except OSError:
                pass # don't panic on broken links

            if not o.dryrun: 
                tfp.add(fp)

    runstats.tarfiles+=1

    if not o.dryrun:
        tfp.finalize()

    if not o.dryrun and o.validate:
        tfp.validate()
    
'''
rundir: path to run, starting with ?.  E.g. 
arcdir: path to where tarballs should be created: 

'''
def archiveRun(rundir, arcdir):
    runstats=stats()

    o.runname=os.path.basename(rundir)
    starttime=time.time()

    ## arcdir=os.path.abspath(arcdir) ## not needed
    o.started='%s/%s_started.txt' % (arcdir, o.runname)
    o.finished='%s/%s_finished.txt' % (arcdir, o.runname) 

    if Archiver.exists(o.started): # need to fix since dirs don't exist in S3  HERE
        if o.force: 
            logger.warning("%s exists, forcing overwrite" % o.finished)
        else:
            if Archiver.exists(o.finished): 
                logger.debug("%s appears finished, skipping" % arcdir)
                return runstats
            else:
                error("Partial archive of %s exists" % arcdir)
    else:
        logger.debug('getting started %s' % arcdir)  
        if not o.dryrun:
            Archiver.touch(o.started)

    runstats.runs=1
    # set up log file for this run 
    if not o.dryrun:
        runLogFileBN='%s_%s_archive.log' % (o.runname, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime()))
        runLogFile='%s/%s' % (o.staging, runLogFileBN)
        h=logging.FileHandler(runLogFile)
        h.setLevel(logging.DEBUG)
        h.setFormatter(formatter)
        logger.addHandler(h)
    
    logger.info("Archiving %s to %s" % (rundir, arcdir))
    if not os.path.isdir(rundir):
        error("Bad rundir %s" % rundir)

    o.tmpdir=os.path.abspath(o.tmpdir)
    if not os.path.isdir(o.tmpdir):
        error("Bad tmpdir %s" % o.tmpdir)

    # cd to dir above rundir
    os.chdir(rundir); os.chdir('..')

    makeTarball(o.runname, arcdir, "%s_%%s" % o.runname, runstats)

    if not o.dryrun: 
        Archiver.touch(o.finished)
    t=time.time()-starttime
    bw=float(runstats.bytes)/(1024.0**2)/t
    logger.info("All Done %d Tarfiles, %d Files, %d quips, %f GB, %f Sec, %f MB/sec" % (runstats.tarfiles, runstats.files, runstats.quips, float(runstats.bytes)/1024**3, t, bw))
    if not o.dryrun: 
        logger.removeHandler(h)
        Archiver.moveFile(runLogFile, f'{arcdir}/{runLogFileBN}') 
    return runstats

'''
The novaseqX uses 8 digits for the timestamp in the flowcell name, e.g. 20230808_, where as
earlier machines used 6.  
'''

def getRundate(run):
    mo=re.match(r'^(\d+)_',run)
    if not mo:
        error('bad rundate')
    datestr=mo.group(1)    
    if len(datestr)==8:
        return datestr[2:]
    else:
        return datestr
    
if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("--novalidate", dest="validate", action="store_false", default=True, help="don't validate")
    parser.add_argument("-p", "--projecttars", dest="projecttars", action="store_true", default=True, help="put projects into separate tars")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force to overwrite tar or finished files")
    parser.add_argument("-t", "--tmpdir", dest="tmpdir", default="/tmp", help="where to create tmp files")
    parser.add_argument("-i", "--infile", dest="infile", help="file containing runs to archive")
    parser.add_argument("-r", "--rundir", dest="rundir", help="run directory")
    parser.add_argument("-a", "--arcdir", dest="arcdir", default="archive", help="archive directory") 
    parser.add_argument("--cuton", dest="cuton", help="date cuton; a run earlier than this 6 digit date will not be archived.  E.g. 150531.  Negative numbers are interpreted as days in the past, e.g. -45 means 45 days ago.")
    parser.add_argument("-c", "--cutoff", dest="cutoff", help="date cutoff; a run later than this will no be archived.  Similar to --cuton")
    parser.add_argument("-l", "--logfile", dest="logfile", default="archive", help="logfile prefix")
    parser.add_argument("--testlen", dest="testlen", type=int, default=10000, help="number of bytes to validate from each file")
    parser.add_argument("--maxthds", dest="maxthds", type=int, default=20, help="max threads")
    parser.add_argument("--maxsum", dest="maxsum", type=int, default=200, help="max memory to use (GBytes)")
    parser.add_argument("--staging", dest="staging", default=tempfile.mkdtemp(prefix='/home/rdb9/palmer_scratch/staging/'), help="staging prefix of dir for tars and log file")

    o=parser.parse_args()

    starttime=time.time()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf=logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose: hf.setLevel(logging.DEBUG)
    logger.addHandler(hf)

    # do some validation
    # require exactly one of -r, --automatic, -i
    if countTrue(o.rundir, o.automatic, o.infile) != 1:
        error("Must specify exactly one of -r --automatic -i")
    
    if o.automatic:
        if not o.cuton: o.cuton=-180
        if not o.cutoff: o.cutoff=-60

    if o.cuton: o.cuton=fixCut(o.cuton)
    if o.cuton: o.cutoff=fixCut(o.cutoff)

    if o.cuton and o.cutoff and o.cutoff < o.cuton:
        error("--cuton must be less than --cutoff")

    logger.debug("Invocation: " + " ".join(sys.argv))
    logger.debug("Cwd: " + os.getcwd())
    logger.debug("Options:" + str(o))

    totalstats=stats()
    runs=[]

    if o.rundir:
        runs=[o.rundir]

    elif o.infile:
        fp=open(o.infile)
        for l in fp:
            # denotes comments
            if l.startswith('#'): continue
            idx=l.find('#')
            if idx!=-1:
                l=l[:idx]

            runs.append(l.strip())

    elif o.automatic:
        rds=['/ycga-ba/ba_sequencers[12356]/sequencer?/runs/[0-9]*', '/ycga-gpfs/sequencers/illumina/sequencer*/runs/[0-9]*']
        runs=sorted(reduce(lambda a,b: a+b, [glob.glob(rd) for rd in rds]))

    passedruns=[]
    for run in runs:
        # original deleted run (just a file)
        if run.endswith(".DELETED"):
            logger.debug("Skipping %s: deleted", run) 
            continue
        # new style deleted run (reduced tree)
        if run.endswith(".deleted"):
            logger.debug("Skipping %s: deleted", run) 
            continue
        rundate=getRundate(os.path.basename(run))
        if o.cuton and rundate < o.cuton:
            logger.debug("Skipping %s: earlier than cuton" % run)
            continue
        if o.cutoff and rundate > o.cutoff:
            logger.debug("Skipping %s: later than cutoff" % run)
            continue
        passedruns.append(run)
    runs=passedruns

    # ok, here we go
    cwd=os.getcwd()
    logger.info("Going to archive %d runs" % len(runs))

    # create archiver
    #Archiver=S3Interface.client(logger)
    Archiver=GlobusInterface.client(logger, "ad28f8d7-33ba-4402-804e-3f454aeea842", "924c6f20-aa6f-41ef-bfdf-ada650163378")

    for run in runs:
        arcdir=mkarcdir(run, o.arcdir) # returns path to archive directory for this run
        runstats=archiveRun(run, arcdir) # do the archiving
        totalstats.comb(runstats)
        
        os.chdir(cwd) # archiveRun changed our dir, change it back now

    t=time.time()-starttime
    bw=float(totalstats.bytes)/(1024.0**2)/t

    logger.debug("Removing staging dir %s" % o.staging)
    shutil.rmtree(o.staging)
    logger.info("Archiving Finished %d Runs, %d Tarfiles, %d Files, %d quips, %f GB, %f Sec, %f MB/sec" % (totalstats.runs, totalstats.tarfiles, totalstats.files, totalstats.quips, float(totalstats.bytes)/1024**3, t, bw))
