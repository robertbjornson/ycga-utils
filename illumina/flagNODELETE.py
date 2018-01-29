
import glob, argparse, logging, itertools, os.path, time

''' 
This utility can be used to prevent illumina runs from being deleted by delete.py

Runs will not be deleted if a file exists in the top level of the run called NODELETE*

By convention, if we want to prevent any run containing samples submitted by Sb32 from being deleted, we'd
look for Project_Sb32, and create NODELETE.Project_Sb32

'''
'''
runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']
'''

runlocs=["/home/rob/project/tools/ycga-utils/illumina/FAKERUNS/sequencers/sequencer?/runs/*",]

if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-p", "--project", dest="project", help="flag runs containing this netid as project")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="don't actually delete")
    parser.add_argument("-l", "--logfile", dest="logfile", default="flag", help="logfile prefix")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="be verbose")

    o=parser.parse_args()

    # set up logger
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc = logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose:
        hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf = logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    hf.setLevel(logging.DEBUG)
    logger.addHandler(hf)

    logger.info("Flagging Started")

    runs=itertools.chain.from_iterable([glob.glob(loc) for loc in runlocs])
    for r in runs:
        logger.info("Checking %s" % r)
        if o.project:
            found=glob.glob(r+'/Data/Intensities/BaseCalls/Unaligned*/Project_%s'% o.project)
            if found:
                logger.info("Found %s in %s, flagging" % (found, r))
                if not o.dryrun:
                    with open(os.path.join(r, "NODELETE.Project_%s" % o.project), "w") as tfp:
                        tfp.write("")
        
    logger.info("Flagging Done")
