
import os, tarfile, subprocess, logging, argparse, sys, re, tempfile, time, threading, hashlib, gzip, glob, datetime, shutil
from functools import reduce


epilog="epilog"

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

def doRm(e):
    lst=glob.glob(e)
    if o.dryrun:
        logger.debug(f"removing {e}")
        return
    
    for t in lst:
        if os.path.isfile(t):
            os.remove(t)
        elif os.path.isdir(t):
            os.rmtree(t)

def cleanRun(r):
    logger.info(f'Cleaning {r}')
    doRm(f'{r}/Data/Intensities/BaseCalls/Undetermined*') 
    doRm(f'{r}/Data/Intensities/BaseCalls/L00?')
    doRm(f'{r}/Data/Intensities/L00?')
    doRm(f'{r}/ThumbnailImages')
    doRm(f'{r}/Images')
    cleaned=f'{r}/cleaned.txt'
    if not o.dryrun: open(cleaned, 'w').close()

if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-i", "--infile", dest="infile", help="file containing runs to clean")
    parser.add_argument("-r", "--rundir", dest="rundir", help="a single run directory to clean")
    parser.add_argument("--cutoff", dest="cutoff", default="-45", help="date cutoff; a run later than this 6 digit date will not be cleaned.  E.g. 150531.  Negative numbers are interpreted as days in the past, e.g. -45 means 45 days ago.")
    parser.add_argument("-l", "--logfile", dest="logfile", default="clean", help="logfile prefix")
    parser.add_argument("-f", "--force", dest="force", action="store_true", default=False, help="force clean")

    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('clean')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf=logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose: hf.setLevel(logging.DEBUG)
    logger.addHandler(hf)

    # do some validation
    # require exactly one of -r, --automatic, -i
    if countTrue(o.rundir, o.automatic, o.infile) != 1:
        error("Must specify exactly one of -r --automatic -i")
    
    if o.cutoff: o.cutoff=fixCut(o.cutoff)

    # sanity check to avoid accidents
    mindays=45
    now=int(time.strftime("%y%m%d", time.gmtime()))
    if ((now-int(o.cutoff))<mindays):
        error("--cutoff must be at least %s days in the past" % mindays)
    
    logger.debug("Invocation: " + " ".join(sys.argv))
    logger.debug("Cwd: " + os.getcwd())
    logger.debug("Options:" + str(o))

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
        #/ycga-ba/ba_sequencers6/sequencerX/runs/160318_D00536_0228_AC8H4JANXX
        #rds=['/ycga-ba/ba_sequencers[6]/sequencer?/runs/[0-9]*']
        #rds=['/ycga-ba/ba_sequencers[12356]/sequencer?/runs/[0-9]*']
        logger.debug("Automatic pat is %s" % str(rds))
        runs=sorted(reduce(lambda a,b: a+b, [glob.glob(rd) for rd in rds]))

    # clean runs(remove L00? dirs, including bcl files)

    cleanjobs=[]
    for run in runs:
        if os.path.isfile("%s/NOCLEAN" % run):
            logger.info("Not cleaning %s, NOCLEAN file found" % run)
            continue

        if run.endswith(".DELETED"):
            logger.debug("%s deleted, skipping" % run)
            continue

        rundate=getRundate(os.path.basename(run))
        if o.cutoff < rundate:
            logger.debug("%s too recent" % run)
            continue

        cleaned='%s/cleaned.txt' % (run,)
        if os.path.exists(cleaned) and not o.force:
            logger.debug("Already cleaned %s" % run)
            continue

        cleanjobs.append(run)

    logger.info("Found %d runs to clean" % len(cleanjobs))

    for r in cleanjobs:
        cleanRun(r)

