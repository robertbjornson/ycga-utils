
'''
This sets the object class.  
ExtraArgs = {'StorageClass':'INTELLIGENT_TIERING'})

The optional deep archive tier is set at the bucket level. I did it in the console


Maybe set the part size explicitly?

'''

import boto3, botocore, logging, re, os

class client(object):
    def __init__(self, logger, profile='default', endpoint_url='https://s3.amazonaws.com', bucket='ycgaarchivebucket'):
        self.logger = logger
        self.bucket = bucket
        self.profile = profile
        self.endpoint_url = endpoint_url
        self.session=boto3.Session(profile_name=profile)
        self.s3_client = self.session.client('s3', endpoint_url=endpoint_url)

    def __repr__(self):
        return f'S3 Interface: profile {self.profile}; bucket {self.bucket}'

    def exists(self, obj):
        try:
            self.s3_client.get_object(Bucket=self.bucket, Key=obj)
        except: # FIX to be more specific
            return False
        return True
            
    def moveFile(self, srcFile, destObj):
        filesize=os.path.getsize(srcFile)
        self.logger.debug(f"submitting transfer {srcFile}, {destObj}, size {filesize}")
        ret=self.s3_client.upload_file(Filename=srcFile, Bucket=self.bucket, Key=destObj) #, ExtraArgs = {'StorageClass':'INTELLIGENT_TIERING'})
        if not ret:
            self.logger.error("upload failed")
    
    # not used ?
    def getHead(self, obj):
        return self.s3_client.head_object(Bucket=self.bucket, Key=obj)

    def mkDir(self, path): # not needed in S3
        pass

    ''' not used
    def touchObj(self, obj): # for finished file
        self.s3_client.put_object(Bucket=self.bucket, Key=obj)
    '''

    ''' not used
    def listDir(self, dir):
        pat=re.compile(f'^{dir}/([^/]+)$')
        l=self.s3_client.list_objects(Bucket=self.bucket)['Contents']
        return [o for o in l if pat.match(o['Key'])]
    '''

    def fatal(self, msg):
        self.logger.error(msg)
        sys.exit(1)

