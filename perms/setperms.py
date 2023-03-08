
'''.vscode/# stuff here /gpfs/ycga/project/lsprog/rdb9/SeqProtection

TODO:
    - get mapping from netids to groups using wikilims
    - put mapping in google doc??
    - create setfacl and getfacl wrappers (done)
    - parser of getfacl output
    - comparator of existing and wanted facl to produce changes

execgetfacl -t file

# group: lsprog
user::rw-
user:ccc7:rw-
group::rw-
group:mane:rw-
mask::rw-
other::rw-

'''
import re, os, sys, argparse, logging, time, subprocess


def doGetFacl(pth):
    return subprocess.check_output(["./execgetfacl", "-c", pth], text=True)

parseAclPat=re.compile('^(group|user):(\w+):(.+)$')
def parseAcls(s):
    d={"group":{}, "user":{}}
    for l in s.split('\n'):
        if l.startswith("#"): continue
        mo=parseAclPat.match(l)
        if mo:
            typ, name, perms=mo.groups()
            d[typ][name]=perms
    return d
            
def getMapping(f='netid_grps.txt'):
    mapping={}
    for l in open(f):
        netid, grp=l.strip().split('\t')
        mapping[netid]=grp
    return mapping

def doperm(root, mo, mapping):
    logger.info(f"doing {root}")
    key=mo.group(1)
    


epilog = ''

if __name__=='__main__': 

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-a", "--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="verbose")
    parser.add_argument("-d", "--dir", dest="rootdir", default=None, help="root dir")
    parser.add_argument("-l", "--logfile", dest="logfile", default="clean", help="logfile prefix")

    o=parser.parse_args()
    print(o)
    # set up logging
    logger=logging.getLogger('setperms')
    formatter=logging.Formatter("%(asctime)s %(threadName)s %(levelname)s %(message)s")
    logger.setLevel(logging.DEBUG)

    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    if not o.verbose: hc.setLevel(logging.INFO)
    logger.addHandler(hc)

    hf=logging.FileHandler("%s_%s.log" % (o.logfile, time.strftime("%Y_%m_%d_%H:%M:%S", time.gmtime())))
    hf.setFormatter(formatter)
    if not o.verbose: hf.setLevel(logging.DEBUG)

    perm_dirs=['/Project_(\w+)$', ]
    prune_dirs=['L00\d$', ]

    perm_pats=[re.compile(d) for d in perm_dirs]
    prune_pats=[re.compile(d) for d in prune_dirs]

    mapping=getMapping()

    #root="/gpfs/ycga/sequencers/illumina/sequencerC/runs/230201_A01519_0228_AHH7LYDSX5"

    for (root, dirs, files) in os.walk(o.rootdir):
        for prune_pat in prune_pats:
            if prune_pat.search(root):
                logger.info(f'pruning {root}')
                dirs.clear()
                continue
        for perm_pat in perm_pats:
            mo = perm_pat.search(root)
            if mo:
                doperm(root, mo, mapping)
                dirs.clear()
                continue

    print("done")
