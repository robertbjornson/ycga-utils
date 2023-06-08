
import os, sys, os.path, stat, re, pwd

# I think I could figure out the uid of the user and substitute that
OKUSERS=['rdb9', 'ccc7'] #only these users are allowed

CIFSNOBODY=65534 # This is the uid that we get from the anonymous cifs mount.  No username exists for it.

FCFiles=['Data', 'Data/Intensities', 'Data/Intensities/BaseCalls']

ROOT='(/ycga-gpfs|/gpfs/ycga)/sequencers/illumina/sequencer[A-Z]'

def chown(fname):
    owner, gid=getFileOwner(fname)
    if owner not in NFSNOBODY and owner!=MYUID: # remember, we changed BaseCalls, so we will see it
        bailout("found file %s owned by %d, expected %s" % (fname, owner, str(NFSNOBODY)))
    os.chown(fname, MYUID, MYGID)
    
def chmod(fname, mode):
    os.chmod(fname, mode)
    
def getFileOwner(fname):
    stats=os.stat(fname)
    return (stats[stat.ST_UID],stats[stat.ST_GID])

def bailout(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)
    
if __name__=='__main__':
    if len(sys.argv)!=2:
        bailout("Usage %s FC" % sys.argv[0])
        
    FC=os.path.abspath(sys.argv[1])
    todir=FC.replace('incoming', 'runs')

    NFSNOBODY=[1001,]  # 1001 is sbsuser
    
    # checks
    # restrict this script to certain uses
    MYUID=os.getuid()
    MYGID=os.getgid()

    okuids=[pwd.getpwnam(u)[2] for u in OKUSERS]
    
    if MYUID not in okuids:
        bailout("You are not allowed to run this script")

    # check that FC looks like a FC
    for fcfile in FCFiles:
        chkfile=FC + os.sep + fcfile
        if not os.path.exists(chkfile):
            bailout("Expected to find %s, not found" % chkfile)

    # check that FC is owned by NFSNOBODY
    uid,gid=getFileOwner(FC)
    if uid not in NFSNOBODY:
        bailout("Expected FC to be owned by %s, failed" % str(NFSNOBODY))

    # check that FC looks like what we expect
    patstr='%s/incoming' % ROOT
    pat=re.compile(patstr)
    if not pat.match(FC):
        bailout("Expected FC path to be like %s, was %s" % (patstr, FC))

    # check that todir looks like what we expect
    patstr='%s/runs' % ROOT
    pat=re.compile(patstr)
    if not pat.match(todir):
        bailout("Expected todir to be like %s, was %s" % (patstr, todir))

    # make sure that todir doesn't exist already
    if os.path.exists(todir):
        bailout("Destination dir aready exists.")
        
    print ("Everything looks ok.  Summary:")
    print(("Userid is %d." % MYUID))
    print(("Moving %s to %s and setting ownership and permissions." % (FC, todir)))

    print(("Everything ok? (N/y) ",))
    ans=sys.stdin.readline()[0]
    if ans not in 'yY':
        bailout("Ok, bailing out")

    print ("OK, here we go")
    os.rename(FC,todir)
    
    BaseCallDir=os.sep.join([todir, 'Data', 'Intensities', 'BaseCalls'])
    chown(BaseCallDir)
    print ("Fixed BaseCalls")

    InterOpDir=os.sep.join([todir, 'InterOp'])
    chown(InterOpDir)
    print ("Fixed InterOp")

    print ("You can run analysis now.  This version doesn\'t do anything to other files.")

    
    #for dirpath, dirnames, filenames in os.walk(todir):
    #    # skip BaseCalls, because I own it already, and besides it's changing under me, which can cause failure below
    #    if "BaseCalls" in dirnames:
    #        dirnames.remove("BaseCalls")
    #    chown(dirpath)
    #    for fn in filenames:
    #        chown(dirpath+os.sep+fn)
    #        chmod(dirpath+os.sep+fn, 0644) # THE ZERO IS IMPORTANT!@!

    sys.exit(0)
