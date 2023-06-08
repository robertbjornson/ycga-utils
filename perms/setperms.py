
'''.vscode/# stuff here /gpfs/ycga/project/lsprog/rdb9/SeqProtection


asdfasdf



TODO:
    - get mapping from netids to groups using wikilims (done)
    - put mapping in google doc (done)
    - create setfacl and getfacl wrappers (done)
    - parser of getfacl output (done)
    - comparator of existing and wanted facl to produce changes (never mind)
    - add default perms: g:ycga_admins:rX, o:  (done)
    - check on weird netid syntax rdb9-1, or ccc7-rdb9
    - exception paths
    - separate logging and errors (not now)
    - refine "automatic"

execgetfacl -t file

# group: lsprog
user::rw-
user:ccc7:rw-
group::rw-
group:mane:rw-
mask::rw-
other::rw-

ldapsearch -xLLL -D cn=client,o=hpc.yale.edu -w hpc@Client '(&(objectClass=posixGroup)(pi=uid=smm68,ou=People,o=hpc.yale.edu))' cn
dn: cn=mane,ou=Groups,o=hpc.yale.edu
cn: mane

dn: cn=ycga,ou=Groups,o=hpc.yale.edu
cn: ycga


setfacl examples:
    setfacl -x g:foo:wrX # remove this entry
    setfacl -m g:foo     # remove foo's entry
    setfacl -b  # remove all
'''
import re, os, sys, argparse, logging, time, subprocess, gspread
import pandas as pd

'''
Got this from 
https://medium.com/geekculture/2-easy-ways-to-read-google-sheets-data-using-python-9e7ef366c775

'''

defaultPerms=['g:ycga_admins:rX','o::-']

#automatic_paths=['/gpfs/ycga/project/lsprog/rdb9/repos/ycga-utils/perms/testdata','/gpfs/ycga/sequencers/illumina', '/gpfs/gibbs/pi/ycga/pacbio/gw92/10x/Single_Cell']
automatic_paths=['/gpfs/ycga/project/lsprog/rdb9/repos/ycga-utils/perms/testdata',]

def getGD(sheet):
    SHEET_ID = '1V7gLL7RXsgYwZps-0t3oyczey-e5ebXj9Vg_LquvKgA'
    gc = gspread.service_account('ycga-stuff-66fb0d0b8528.json')
    spreadsheet = gc.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(sheet)
    rows = worksheet.get_all_records()
    df = pd.DataFrame(rows)
    df=df.set_index("entry")
    return df


def doGetFacl(pth):
    return parseAcls(subprocess.check_output(["./execgetfacl", "-c", pth], text=True))

parseAclPat=re.compile('^(group|user):(\w*):(.+)$')
def parseAcls(s):
    d={"group":[], "user":[]}
    for l in s.split('\n'):
        if l.startswith("#"): continue
        mo=parseAclPat.match(l)
        if mo:
            typ, name, perms=mo.groups()
            d[typ].append(name)
    return d

'''
root is path to file
mo is match object.  mo.groups(1) should be the netid
mapping is the df mapping netids to groups and users
'''

def doperm(pth, rec):
    gs=rec['groups'].split(':') if rec['groups'] else []
    us=rec['users'].split(':') if rec['users'] else []
    perms=defaultPerms[:]
    for g in gs:
        perms.append(f'g:{g}:rX')
    for u in us:
        perms.append(f'u:{u}:rX')
    if perms:
        permstring=','.join(perms)
        cmd=f'./execsetfacl -b -m {",".join(perms)} {root}'
        logger.info(cmd)
        if not o.dryrun:
            ret=subprocess.call(cmd, shell=True)
            if ret:
                logger.error(f'{cmd} failed: {ret}')
    else:
        logger.info(f"Nothing to do: {pth}")


epilog = ''

if __name__=='__main__': 

    parser=argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-a", "--automatic", dest="automatic", action="store_true", default=False, help="automatic settings")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="verbose")
    parser.add_argument("-d", "--dir", dest="d", default=None, help="root dir")
    parser.add_argument("-l", "--logfile", dest="logfile", default="setperms", help="logfile prefix")
    parser.add_argument("-n", "--dryrun", dest="dryrun", action="store_true", default=False, help="dont do anything")

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
    logger.addHandler(hf)

    perm_dirs=['/Unaligned.*/Project_(\w+)$', '/10x/Single_Cell/([a-zA-Z]{1,4}[0-9]{1,4})(?:-\w+)*$']
    prune_dirs=['L00\d$', ]

    perm_pats=[re.compile(d) for d in perm_dirs]
    prune_pats=[re.compile(d) for d in prune_dirs]

    rulesMapping=getGD("Rules")
    exceptionsMapping=getGD("Exceptions")
    '''
    df.loc['ccc7']
    '''

    if o.automatic:
        paths=automatic_paths
    elif o.d:
        paths=[d,]
    else:
        logger.error("must provide -a or -d")
        SystemExit()

    #root="/gpfs/ycga/sequencers/illumina/sequencerC/runs/230201_A01519_0228_AHH7LYDSX5"
    for rootdir in paths:
        for (root, dirs, files) in os.walk(rootdir):
            for prune_pat in prune_pats:
                if prune_pat.search(root):
                    logger.debug(f'pruning {root}')
                    dirs.clear()
                    continue
            if root in exceptionsMapping.index:
                rec=exceptionsMapping.loc[root]
                doperm(root, rec)
                dirs.clear()
                continue
            for perm_pat in perm_pats:
                mo = perm_pat.search(root)
                if mo:
                    key=mo.group(1).lower()
                    try:    
                        rec=rulesMapping.loc[key]
                    except KeyError:
                        logger.error(f"Found missing netid {key} {root}")
                        dirs.clear()
                        continue
                    doperm(root, rec)
                    dirs.clear()
                    continue

    logger.info("Finished")
