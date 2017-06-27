
import os, hashlib, sys, datetime, pwd, grp

def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()

def doFile(fn):
    sm=md5sum(fn)
    stat=os.stat(fn)
    try:
        user=pwd.getpwuid(stat.st_uid)[0]
    except KeyError:
        user=str(stat.st_uid)
                
    try:
        group=grp.getgrgid(stat.st_gid)[0]
    except KeyError:
        group=str(stat.st_gid)

    dt=datetime.datetime.fromtimestamp(stat.st_mtime)
    sz=stat.st_size
    return (fn, user, group, str(sz), str(dt), sm)


def genFiles(top):
    for d, dirs, files in os.walk(top):
        for f in files:
            yield os.path.join(d, f)
    
def parListTree(pool, top, chunksize=10):
    recs=pool.map(doFile, genFiles(top), chunksize)
    return recs

def listTree(top):
    recs=map(doFile, genFiles(top))
    return recs

if __name__=='__main__':
    for l in listTree(sys.argv[1]):
        print ("\t".join(l))
