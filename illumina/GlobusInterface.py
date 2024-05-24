import logging, globus_sdk, random, sys
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
            self.logger.debug(f'exists returned {stat}')
            return True
        except TransferAPIError:
            return False

    def moveFile(self, src_file, dest_file):
        
        task_data = globus_sdk.TransferData(source_endpoint=self.local_collection_id, destination_endpoint=self.remote_collection_id, verify_checksum=True)

        # because the globus share is rooted at /home/rdb9/palmer_scratch/staging/, we need to remove that prefix from the src_file!
        src_file=src_file.replace('/home/rdb9/palmer_scratch/staging/', '') ## NOT Quite, fix
        self.logger.debug(f"adding {src_file}, {dest_file}")
        task_data.add_item(src_file, dest_file)

        # submit, getting back the task ID
        self.logger.debug(f"submitting transfer {src_file}, {dest_file}")
        task_doc = self.tc.submit_transfer(task_data)
        task_id = task_doc["task_id"]
        
        self.logger.debug(f"waiting on {task_id}")
        while not self.tc.task_wait(task_id, timeout=60):
            self.logger.debug(f"waiting on {task_id}")
        self.logger.debug("Finished")

    def getHead(self, obj):
        return False

    def touch(self, fn):
        self.logger.debug(f"touching {fn}")
        return self.moveFile('dummy.txt', fn)
        
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
filenames=['/1M.dat', '/10M.dat', '/100M.dat']
destdir='/Testing'

if __name__=='__main__':

    logger=logging.getLogger('archive')
    formatter=logging.Formatter("%(asctime)s %(message)s")
    logger.setLevel(logging.DEBUG)
    hc=logging.StreamHandler()
    hc.setFormatter(formatter)
    logger.addHandler(hc)

    c=client(logger, src_collection_id, dest_collection_id)

    newfiles=[f'{destdir}{fn}.{random.randint(1,10000)}' for fn in filenames]

    ret=c.exists(destdir)
    if ret:
        logger.debug(f"destdir {destdir} already there")
    else:
        logger.debug(f"creating {destdir}")
        c.mkDir(destdir)
                     
    logger.debug("testing transfer")
    for sf, df in zip(filenames, newfiles):
        c.moveFile(filenames, newfiles)

    logger.debug("testing exists")
    for nf in newfiles:
        c.exists(nf)
