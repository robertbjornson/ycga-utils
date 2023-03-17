'''.vscode/

2 step process to get all groups for a user
[tt343@login1.mccleary ~]$ ldapsearch -xLLL -D cn=client,o=hpc.yale.edu -w hpc@Client '(&(objectClass=posixAccount)(uid=rdb9))' gidNumber
dn: uid=rdb9,ou=People,o=hpc.yale.edu
gidNumber: 11133

[tt343@login1.mccleary ~]$ ldapsearch -xLLL -D cn=client,o=hpc.yale.edu -w hpc@Client '(&(objectClass=posixGroup)(gidNumber=11133))' cn
dn: cn=support,ou=Groups,o=hpc.yale.edu
cn: support
'''

import sys, subprocess, re, datetime

def getGroupViaId(netid):
    try:
        s=subprocess.check_output(f'id -gn {netid}', shell=True, text=True)
    except:
        s=""
    return s.strip().split()

def getPIGroupsViaLdap(netid):
    pat=re.compile('cn: (\w+)$')
    grps=[]
    s=subprocess.check_output(f"ldapsearch -xLLL -D cn=client,o=hpc.yale.edu -w hpc@Client '(&(objectClass=posixGroup)(pi=uid={netid},ou=People,o=hpc.yale.edu))' cn", 
        shell=True, text=True)
    for l in s.split('\n'):
        mo=pat.match(l)
        if (mo):
            grps.append(mo.group(1))
    return grps
    
def getWikilimsData():
    s=subprocess.check_output("wget -O - http://wikilims.ycga.yale.edu:6789/accountDump.rpy", shell=True, text=True)
    return s

hdr="netid, netid groups, pi netid, pi groups, users, date, created by, notes\n"

def dump_table(tbl, fn):
    ofp=open(fn, "w")
    now=datetime.date.today().strftime("%Y-%m-%d")
    ofp.write(hdr)
    for l in tbl:
        ofp.write(f'{l[0]},')
        ofp.write(f'{":".join(l[1])},')
        ofp.write(f'{l[2]},')
        ofp.write(f'{":".join(l[3])},,')
        ofp.write(f'{now},wikilims,\n')
        

if __name__=='__main__':
    wd=getWikilimsData()
    tbl=[]
    for l in wd.strip().split('\n'):
        netid, pi_netid = l.strip().split('\t')
        print(f'doing {netid}')
        netid=netid.lower(); pi_netid=pi_netid.lower()
        netid_grps = getGroupViaId(netid)
        pi_netid_grps = getPIGroupsViaLdap(pi_netid)
        tbl.append([netid, netid_grps, pi_netid, pi_netid_grps])
    dump_table(tbl, "tbl.txt")
    