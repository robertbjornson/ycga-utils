import glob, itertools, os, logging, argparse, time, shutil, subprocess, re, sys
from multiprocessing import Pool

import listTree

def fltr(r):
    rn=os.path.basename(r)
    try:
        dt=int(rn[:6])
        return dt < o.cutoff
    except:
        print ("weirdly named run: %s" % r)
        return False

runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']

#runlocs=['/ycga-gpfs/sequencers/illumina/sequencerX/runs/*',]

equiv=(
    ("/ycga-gpfs/sequencers/panfs/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/"),
    ("/ycga-gpfs/sequencers/panfs/sequencers1/", "/SAY/archive/YCGA-729009-YCGA/archive/panfs/sequencers/"),
    ("/ycga-ba/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-ba/"),
    ("/ycga-gpfs/sequencers/illumina/", "/SAY/archive/YCGA-729009-YCGA/archive/ycga-gpfs/sequencers/illumina/")
)

'''

runlocs=["/home/rob/project/tools/ycga-utils/illumina/FAKERUNS/sequencers/sequencer?/*",]

equiv=(
    ("/home/rob/project/tools/ycga-utils/illumina/FAKERUNS", "/home/rob/project/tools/ycga-utils/illumina/FAKEARCHIVE"),
)
'''

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



def getTrashDir(r):
    mo=re.match('(/ycga-gpfs/sequencers/illumina/|/ycga-ba/ba_sequencers\d/+|/ycga-gpfs/sequencers/panfs/)', r)
    td=mo.group(1)+"TRASH"
    if os.path.isdir(td):
        return td
    else:
        logger.error("Trash dir %s doesn't exist" % td)
        raise Exception("No trash dir")
    
if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-c", "--cutoff", dest="cutoff", type=int, required=True, help="cutoff date YYMMDD")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=True, help="don't actually delete")
    parser.add_argument("-l", "--logfile", dest="logfile", default="delete", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-i", "--interactive", dest="interactive", action="store_true", default=False, help="ask before each deletion")
    parser.add_argument("-p", "--pattern", dest="pattern", default=".*", help="delete runs matching pattern")
    parser.add_argument("--nosum", dest="dosum", default=True, action="store_false", help="don't do checksum")
    parser.add_argument("-P", "--procs", dest="procs", type=int, default=1, help="number of procs to use for checksumming")

    o=parser.parse_args()
    starttime=time.time()

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
        hf.setLevel(logging.DEBUG)

    logger.addHandler(hf)

    logger.info("here")

    runs=itertools.chain.from_iterable([glob.glob(loc) for loc in runlocs])

    delruns=[r for r in runs if fltr(r)]

    pat=re.compile(o.pattern)
    delruns=[r for r in delruns if pat.search(r)]
    
    deletedcnt=0
    deletecnt=0
    missingcnt=0
    tot_files_rm=0
    tot_bytes_rm=0

    pool=Pool(o.procs)

    for r in delruns:
        if r.endswith(".DELETED"):
            logger.info("Previously deleted %s" % (r,))
            deletedcnt+=1
            # already deleted
            continue
        a=chkArchive(r);
        if not a:
            logger.info("No archive for %s" % r)
            missingcnt+=1
            continue
        else:
            logger.info("Deleting %s" % (r,))
            if o.interactive:
                print ("Go ahead [Yn]?")
                resp=input()
                if resp.lower()=='n': continue

            deletecnt+=1
            if not o.dryrun:
                delfp=open(r+'.DELETED', 'w')
            else:
                delfp=open(os.path.basename(r)+".DELETED", 'w')

            delfp.write("Run deleted %s\n" % time.asctime())
            delfp.write("Archive is here: %s\n" % os.path.dirname(a))
            delfp.write("Files deleted:\n")
            delfp.flush()
            bytes_rm=0
            files_rm=0
            if o.dosum:
                sums=listTree.parListTree(pool, r)
                for s in sums:
                    delfp.write("\t".join(s)+'\n')
                    bytes_rm+=int(s[3])
                    files_rm+=1

                tot_files_rm+=files_rm
                tot_bytes_rm+=bytes_rm
                
            trashdir=getTrashDir(r)
            logger.info("Trashdir is %s" % trashdir)

            #if not o.dryrun: shutil.rmtree(r)
            delfp.write("Done with %s: %d files, %d bytes\n" % (r, files_rm, bytes_rm))
            delfp.close()


    logger.info("All done.  Previous deleted %d, Archive missing %d, Deleted now %d runs %d files %d bytes." % (deletedcnt, missingcnt, deletecnt, tot_files_rm, tot_bytes_rm))

''' Todo
x count and report size of deletions
change rmtree to mv to Trash in same FS:
  -> /ycga-gpfs/sequencers/illumina/TRASH
     /ycga-ba/ba_sequencers#/TRASH
     /ycga-gpfs/sequencers/panfs/
 
think about what to leave behind
'''
