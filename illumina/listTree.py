'''
listTree returns a list of records, where each record describes one file:
fn, owner, group, size, date, checksum (optional)

A multiprocessing pool can be used to do the checksums in parallel.
'''

import os, hashlib, sys, datetime, pwd, grp, gzip

def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with gzip.open(filename, "rb") if filename.endswith(".gz") else open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()

''' Return 6 fields of info about file'''
def doFile(fn, dosum):
    sm=md5sum(fn) if dosum else 0
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

''' Parallel version
    pool: multiprocessing pool to use for parallel execution
    top: top of directory tree to search for files
    dosum: boolean, should checksum be computed?
    chunksize: job chunking for multiprocessing
'''
def parListTree(pool, top, dosum, chunksize=10):
    recs=pool.map(lambda top: doFile(top, dosum), genFiles(top), chunksize)
    return recs

''' Sequential version
    top: top of directory tree to search for files
    dosum: boolean, should checksum be computed?
'''
def listTree(top, dosum):
    recs=map(lambda top: doFile(top, dosum), genFiles(top))
    return recs

if __name__=='__main__':
    for l in listTree(sys.argv[1]):
        print ("\t".join(l))
