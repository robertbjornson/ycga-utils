
import tempfile, subprocess, logging

tmplt='''#!/bin/bash
#SBATCH --array %(range)s
#SBATCH --job-name %(name)s
#SBATCH --time %(maxtime)s
#SBATCH -o %(statusdir)s/slurm-%%A_%%a.out

# DO NOT EDIT LINE BELOW
/vast/palmer/apps/avx2/software/dSQ/1.05/dSQBatch.py --job-file %(jobfilename)s --status-dir %(statusdir)s
'''



def runJobs(jobs, o, name='archivejobs'):
    logger=logging.getLogger('archive')
    d=locals()
    d['range']=f'0-{len(jobs)-1}%{o.maxthds}'        
    d['jobfile']=tempfile.NamedTemporaryFile(dir=o.staging, mode='w', delete=o.clean)
    batchfile=tempfile.NamedTemporaryFile(dir=o.staging, mode='w', delete=o.clean)
    d['jobfilename']=d['jobfile'].name
    d['statusdir']=o.staging
    d['maxtime']=o.maxtime
    for job in jobs:
        d['jobfile'].write(job+'\n')
    d['jobfile'].flush()
    batchfile.write(tmplt%d)
    batchfile.flush()
    cmd=f'/opt/slurm/current/bin/sbatch --wait {batchfile.name}'
    logger.debug(f'running {cmd}')
    ret=subprocess.run(cmd, shell=True)
    return ret.returncode
    
if __name__=='__main__':
    jobs=[f'echo {i}; sleep 5' for i in range(10)]
    o={"maxthds":4, "clean":False, "staging":"."}
    runJobs(jobs, o)
    
