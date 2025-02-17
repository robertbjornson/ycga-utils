import logging, globus_sdk, random, sys, os, tempfile
from globus_sdk import TransferAPIError

class client(object):
    def __init__(self, logger, local_collection_id, remote_collection_id):

        # you must have a client ID
        self.CLIENT_ID = "b205d5dc-4ace-47bf-9fb2-65e3cc2a2514"
        # the secret, loaded from wherever you store it
        # FIX!
        CLIENT_SECRET = "amBVOn5pqvpFwSKzLPiYvH4lqFT9A8KD8JENCiAwui4="

        client = globus_sdk.ConfidentialAppAuthClient(self.CLIENT_ID, CLIENT_SECRET)
        token_response = client.oauth2_client_credentials_tokens()

        # the useful values that you want at the end of this
        globus_auth_data = token_response.by_resource_server["auth.globus.org"]
        globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
        globus_auth_token = globus_auth_data["access_token"]
        globus_transfer_token = globus_transfer_data["access_token"]

        self.local_collection_id=local_collection_id
        self.remote_collection_id=remote_collection_id
        self.tc = globus_sdk.TransferClient(authorizer=globus_sdk.AccessTokenAuthorizer(globus_transfer_token))
        self.logger = logger

    def __repr__(self):
        return f"Globus Interface: CLIENT_ID {self.CLIENT_ID}"

    def exists(self, f):
        try:
            stat=self.tc.operation_stat(self.remote_collection_id, f)
            return True
        except TransferAPIError:
            return False

    def moveFile(self, src_file, dest_file, extra={}):
        
        task_data = globus_sdk.TransferData(source_endpoint=self.local_collection_id, destination_endpoint=self.remote_collection_id, verify_checksum=True)
        filesize=os.path.getsize(src_file)
        
        # because the globus share is rooted at /home/rdb9/palmer_scratch/staging/, we need to remove that prefix from the src_file!
        src_file=src_file.replace('/home/rdb9/palmer_scratch/staging/', '') ## NOT Quite, fix
        self.logger.debug(f"adding {src_file}, {dest_file}")
        task_data.add_item(src_file, dest_file)

        # submit, getting back the task ID
        self.logger.debug(f"submitting transfer {src_file}, {dest_file}, size {filesize}")
        task_doc = self.tc.submit_transfer(task_data)
        task_id = task_doc["task_id"]

        prevsize=0; size=0; pct=0
        self.logger.debug(f"waiting on {task_id}")
        while not self.tc.task_wait(task_id, timeout=60):
            try:
                stat=self.tc.operation_stat(self.remote_collection_id, f"/{dest_file}")
                size=stat["size"]
                pct=100*size/filesize
                
            except:
                self.logger.debug("stat failed")
            self.logger.debug(f"waiting on {task_id}: current size {size} {pct:.2f}%")
            self.logger.debug
            if not size>prevsize:
                self.logger.warning(f"{dest_file} not growing!")

        self.logger.debug("Finished")
        
    def getHead(self, obj):
        return False

    ''' not used
    def touch(self, fn):
        self.logger.debug(f"touching {fn}")
        tf = tempfile.NamedTemporaryFile()
        ret = self.moveFile(tf.name, fn)
        tf.close()
        return ret
    '''
    
    def mkDir(self, path): # not needed in S3
        try:
            self.tc.operation_mkdir(self.remote_collection_id, path)
        except TransferAPIError:
            self.fatal(f'mkdir {path} failed')
            
    def fatal(self, msg):
        self.logger.error(msg)
        sys.exit(1)
        

# testing

# Replace these with your own collection UUIDs

mcc_collection_id="fa56d2d4-adfd-4f1e-b735-5f28bde144d7"
src_collection_id = "64b7e306-edb7-4b17-8b25-9033517eca8b" #ESNET
dest_collection_id = "924c6f20-aa6f-41ef-bfdf-ada650163378" # TESTTARGET

destdir='archive'

srcFiles=['/home/rdb9/palmer_scratch/staging/BIGFILE1M',]
neseTape='23aa87a8-8c58-418d-8326-206962d9e895'


if __name__=='__main__':

    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(message)s")
    logger.setLevel(logging.DEBUG)
    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    logger.addHandler(hc)

    c=client(logger, mcc_collection_id, neseTape)

    newfiles=[f'{destdir}{fn}.{random.randint(1,10000)}' for fn in srcFiles]

    ret=c.exists(destdir)
    if ret:
        logger.debug(f"destdir {destdir} already there")
    else:
        logger.debug(f"creating {destdir}")
        c.mkDir(destdir)
                     
    logger.debug("testing transfer")
    for sf, df in zip(srcFiles, newfiles):
        c.moveFile(sf, df)

    logger.debug("testing exists")
    for nf in newfiles:
        c.exists(nf)
