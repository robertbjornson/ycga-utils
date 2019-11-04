import argparse, os, datetime, time, logging, subprocess, sys


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

def runCmd(cmd):
    logger.debug("running %s" % cmd)
    ret=subprocess.call(cmd, shell=True)
    if (ret!=0):
        logger.error("Cmd failed with %d" % ret)
        sys.exit(ret)

def summarize(counter):
    logger.info("Archived %(archived)s Deleted %(deleted)d Partial %(partial)d" % counter)
 
if __name__=='__main__':

    parser=argparse.ArgumentParser(epilog=info, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("dir")
    parser.add_argument("archDir")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually do anything")
    parser.add_argument("--fastqs", dest="fastqs", action="store_true", default=False, help="only tar *.fastq.gz files")
    parser.add_argument("--archPeriod", dest="archPeriod", type=int, default=30, help="waiting period for archiving")
    parser.add_argument("--delPeriod", dest="delPeriod", type=int, default=365*2, help="waiting period for deletion")
    parser.add_argument("--nodel", dest="nodel", action="store_true", default=False, help="skip actual deletion, but do everything else")
    parser.add_argument("-l", "--logfile", dest="logfile", default="arch_del", help="logfile prefix")

    o=parser.parse_args()

    # set up logging
    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    h=logging.FileHandler('%s_%s.log' % (time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime()), o.logfile))
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
    assert(os.path.isdir(o.archDir))
    now=time.time()

    for d in os.listdir(o.dir):
        src=os.path.join(o.dir, d)
        # skip files
        if not os.path.isdir(src): continue
        ts=int(os.stat(src).st_mtime)
        deltaT=now-ts

        tarFName=os.path.join(o.archDir, d)+".tar"
        finishFName=os.path.join(o.archDir, d)+".finished"
        deletedFName=os.path.join(o.dir, d)+".deleted"

        if deltaT > o.archPeriod * secPerDay:
            if os.path.exists(finishFName):
                logger.debug ("Not archiving %s, already done" % src)
            else:
                if os.path.exists(tarFName):
                    logger.error ("Not archiving %s, tar file %s exists without finish file" % (src, tarFName))
                    counter["partial"]+=1
                    continue # weird; tar file without finish file, don't delete
                else:
                    logger.debug ("archiving %s" % src)
                    counter["archived"]+=1
                    if o.fastqs:
                        cmd="(find %(src)s -name \"*.fastq.gz\" | tar cf %(tarFName)s --files-from=- && touch \"%(finishFName)s\")" % locals()
                    else:
                        cmd="(tar -cf \"%(tarFName)s\" \"%(src)s\" && touch \"%(finishFName)s\")" % locals()
                    if o.dryrun:
                        logger.debug(cmd)
                    else:
                        runCmd(cmd)
        else:
            logger.debug ("not archiving %s, too new" % src)
        if not o.nodel and deltaT > o.delPeriod * secPerDay:
            if os.path.exists(finishFName):
                logger.debug ("deleting %s" % src)
                counter["deleted"]+=1
                cmd="rm -rf \"%s\"" % (src,)            
                msg=deleteTmplt % {'date':(time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())), 'location':tarFName}
                if o.dryrun:
                    logger.debug(cmd)
                else:
                    runCmd(cmd)
                    open(deletedFName, "w").write(msg)
            else:
                logger.error ("not deleting %s, %s not found" % (src, finishFName))
        else:
            logger.debug ("not deleting %s, too new" % src)

    summarize(counter)
