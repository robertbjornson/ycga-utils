#!/home/rob/Installed/anaconda3/bin/python

import glob, itertools, os, logging, argparse, time, shutil, subprocess, re, sys, io, datetime
from multiprocessing import Pool

import listTree

''' This function takes a run name and uses the date component of the name to determine whether
it is earlier than the cutoff date
''' 
def fltr(r):
    rn=os.path.basename(r)
    try:
        return int(rn[:6]) < o.cutoff
    except:
        logger.error("weirdly named run: %s" % r)
        return False


# all the places where runs are found
runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']

#runlocs=['/ycga-gpfs/sequencers/illumina/sequencerX/runs/*',]

# corresonding archive locations
equiv=(
    ("/ycga-gpfs/sequencers/panfs/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/"),
    ("/ycga-gpfs/sequencers/panfs/sequencers1/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/sequencers/"),
    ("/ycga-ba/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-ba/"),
    ("/ycga-gpfs/sequencers/illumina/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-gpfs/sequencers/illumina/")
)


'''
runlocs=["/home/rob/project/tools/ycga-utils/illumina/FAKERUNS/sequencers/sequencer?/runs/*",]

equiv=(
    ("/home/rob/project/tools/ycga-utils/illumina/FAKERUNS", "/home/rob/project/tools/ycga-utils/illumina/FAKEARCHIVE"),
)
'''

''' Check if completed archive exists for some run '''
def chkArchive(r):
    for o, a in equiv:
        if r.startswith(o):
            arun=r.replace(o,a)
            chkfile=arun+'/'+os.path.basename(r)+"_finished.txt"
            st=os.path.exists(chkfile)
            if st:
                logger.debug("check %s %s ok" % (chkfile, st))
                return chkfile
    return None

''' Determine corresponding TRASH dir location for a run, and confirm existence'''
def getTrashDir(r):
    mo=re.match('(/home/rob/project/tools/ycga-utils/illumina/|/ycga-gpfs/sequencers/illumina/|/ycga-ba/ba_sequencers\d/|/ycga-gpfs/sequencers/panfs/)(.*)', r)
    pref=mo.group(1)
    rest=mo.group(2)
    td=os.path.join(pref,"TRASH")
    if os.path.isdir(td):
        return os.path.join(td, rest)
    else:
        logger.error("Trash dir %s doesn't exist" % td)
        raise Exception("No trash dir")

# can't use os.renames because renames prunes the source dirs if empty (ugh)
def myrename(f,t):
    os.makedirs(t)
    os.rename(f,t)

''' This function turns a negative date delta into a 6 digit date that many days in the past.  Anything else is returned unchanged '''
def fixCut(cut):
    try:
        if int(cut) < 0:
            d=datetime.datetime.now() + datetime.timedelta(days=int(cut))
            return int(d.strftime("%y%m%d"))
    except:
        pass
    return cut
    
if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-c", "--cutoff", dest="cutoff", type=int, default=365*2, help="cutoff date (YYMMDD) or -days")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually delete")
    parser.add_argument("-l", "--logfile", dest="logfile", default="delete", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-i", "--interactive", dest="interactive", action="store_true", default=False, help="ask before each deletion")
    parser.add_argument("-p", "--pattern", dest="pattern", default=".*", help="delete runs matching pattern")
    parser.add_argument("--dosum", dest="dosum", default=False, action="store_true", help="do checksum")
    parser.add_argument("-P", "--procs", dest="procs", type=int, default=1, help="number of procs to use for checksumming")
    parser.add_argument("-r", "--runs", dest="runs", help="file containing runs to delete")

    o=parser.parse_args()

    # set up logger
    logger = logging.getLogger('delete')
    formatter = logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc = logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose:
        hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf = logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose:
        hc.setLevel(logging.INFO)
    hf.setLevel(logging.INFO)
    logger.addHandler(hf)

    if o.automatic:
        o.cutoff=-365
    o.cutoff=fixCut(o.cutoff)

    logger.info("Deletion Started")
    logger.info("Cmd: "+" ".join(sys.argv))

    # all runs
    if o.runs:
        with open(o.runs) as fp:
            runs=[l.rstrip() for l in fp]
    else:
        runs=sorted(itertools.chain.from_iterable([glob.glob(loc) for loc in runlocs]))

    # runs passing date cutoff
    delruns=[r for r in runs if fltr(r)]

    # runs matching pattern
    pat=re.compile(o.pattern)
    delruns=[r for r in delruns if pat.search(r)]
    
    deletedcnt=0
    deletecnt=0
    missingcnt=0
    nodeletecnt=0
    tot_files_rm=0
    tot_bytes_rm=0

    pool=Pool(o.procs)

    for r in delruns:
        outbuf=io.StringIO()
        logger.info("Checking %s" % r)
        if os.path.islink(r):
            logger.info("Skipping softlink %s" % (r,))
            continue
        if r.endswith(".DELETED") or r.endswith(".deleted") or (o.dryrun and os.path.exists(os.path.basename(r)+".DELETED")):
            # when dryrunning, .DELETED would be in current dir
            logger.info("Previously deleted %s" % (r,))
            deletedcnt+=1
            # already deleted
            continue
        # any NODELETE* file in run dir prevents deletion
        ndels=glob.glob(os.path.join(r, "NODELETE*"))
        if ndels:
            logger.info("NODELETE(s) %s found in %s, skipping" % (" ".join(ndels), r))
            nodeletecnt+=1
            continue
        a=chkArchive(r)
        if not a: 
            logger.info("No archive for %s" % r)
            missingcnt+=1
            continue
        else:
            logger.info("Deleting %s" % (r,))
            if o.interactive:
                print ("Go ahead [Yn]?")
                resp=input()
                if resp.lower()!='y': continue

            ''' trashdir is the location of the FC dir in trash.  It should not already exist'''
            trashdir=getTrashDir(r)
            logger.debug("Trashdir is %s" % trashdir)

            if os.path.exists(trashdir):
                logger.error("%s exists already, cancelling move" % trashdir)
                continue

            deletecnt+=1

            '''Here's where we finally do something!  For now, move the tree to corresponding TRASH dir, rather than delete'''

            outbuf.write("Run deleted %s\n" % time.asctime())
            outbuf.write("Archive is here: %s\n" % os.path.dirname(a))
            outbuf.write("Files deleted:\n")
            outbuf.flush()
            bytes_rm=0
            files_rm=0
            sums=listTree.listTree(r, o.dosum)
            for s in sums:
                outbuf.write("\t".join(s)+'\n')
                bytes_rm+=int(s[3])
                files_rm+=1

            tot_files_rm+=files_rm
            tot_bytes_rm+=bytes_rm

            # copy file tree to .deleted, without fastq.gzs or Logs
            cmd="rsync -a --exclude '*.fastq.gz' --exclude Logs --exclude RTALogs {}/ {}".format(r, r+".deleted")
            logger.debug("Rsync: {}".format(cmd))
            if not o.dryrun:
                ret=subprocess.call(cmd, shell=True)
                if ret != 0:
                    logger.error("Rsync failed with {}".format(ret))

            logger.debug("Moving %s to %s" % (r, trashdir))
            if not o.dryrun: 
                myrename(r, trashdir)

            outbuf.write("Done with %s: %d files, %d bytes\n" % (r, files_rm, bytes_rm))

            if not o.dryrun:
                # write deleted log into new reduced file tree
                delfp=open(r+".deleted/DELETED", 'w')
            else:
                delfp=open(os.path.basename(r)+".DELETED", 'w') # write dryrun logs here 

            delfp.write(outbuf.getvalue())
            delfp.close()
            outbuf.close()

    logger.info("All done.  Previously deleted %d, Archive missing %d, Deleted now %d runs %d files %d bytes." % (deletedcnt, missingcnt, deletecnt, tot_files_rm, tot_bytes_rm))
