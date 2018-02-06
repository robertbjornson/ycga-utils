
import glob, argparse, logging, itertools, os.path, time

''' 
This utility can be used to prevent illumina runs from being deleted by delete.py

Runs will not be deleted if a file exists in the top level of the run called NODELETE*

By convention, if we want to prevent any run containing samples submitted by Sb32 from being deleted, we'd
look for Project_Sb32, and create NODELETE.Project_Sb32

'''

runlocs=['/ycga-ba/ba_sequencers?/sequencer?/runs/*',
'/ycga-gpfs/sequencers/panfs/sequencers*/sequencer?/runs/*',
'/ycga-gpfs/sequencers/illumina/sequencer?/runs/*']

'''
runlocs=["/home/rob/project/tools/ycga-utils/illumina/FAKERUNS/sequencers/sequencer?/runs/*",]
'''
if __name__=='__main__':

    parser=argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-t", "--tag", dest="tag", required=True, help="use this as tag")
    parser.add_argument("-f", "--file", dest="file", required=True, help="file containing run locations")
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

    for r in open(o.file):
        r=r.strip()
        print("run "+r)
        assert(os.path.isdir(r))
        logger.info("Found %s, flagging" % (r))
        if not o.dryrun:
            with open(os.path.join(r, "NODELETE.%s" % o.tag), "w") as tfp:
                tfp.write("")

    logger.info("Flagging Done")
