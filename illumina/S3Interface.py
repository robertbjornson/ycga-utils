
'''
This sets the object class.  
ExtraArgs = {'StorageClass':'INTELLIGENT_TIERING'})

The optional deep archive tier is set at the bucket level. I did it in the console


Maybe set the part size explicitly?


head object:

>>> client.head_object(Bucket='ycgaarchivebucket', Key='test/10G.dat')
{'ResponseMetadata': {'RequestId': 'ZK5Q802RGJEPV4YK', 'HostId': 'CoXkJFG/y3AiCm0QXrsITG2/onCG4qJ82gX9sQP3f+q2GHClk6p8X0nX/s9twRDmv2ioWuwHiPc=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'CoXkJFG/y3AiCm0QXrsITG2/onCG4qJ82gX9sQP3f+q2GHClk6p8X0nX/s9twRDmv2ioWuwHiPc=', 'x-amz-request-id': 'ZK5Q802RGJEPV4YK', 'date': 'Fri, 02 Aug 2024 14:06:50 GMT', 'last-modified': 'Fri, 26 Jul 2024 18:22:48 GMT', 'etag': '"7eb14036e9a8832af02eee0467e42155-1193"', 'x-amz-server-side-encryption': 'AES256', 'accept-ranges': 'bytes', 'content-type': 'binary/octet-stream', 'server': 'AmazonS3', 'content-length': '10000000000'}, 'RetryAttempts': 0}, 'AcceptRanges': 'bytes', 'LastModified': datetime.datetime(2024, 7, 26, 18, 22, 48, tzinfo=tzutc()), 'ContentLength': 10000000000, 'ETag': '"7eb14036e9a8832af02eee0467e42155-1193"', 'ContentType': 'binary/octet-stream', 'ServerSideEncryption': 'AES256', 'Metadata': {}}


'''

import boto3, botocore, logging, re, os

class client(object):
    def __init__(self, logger, profile='default', endpoint_url='https://s3.amazonaws.com', bucket='ycgasequencearchive', credentials=None):
        self.logger = logger
        self.bucket = bucket
        self.profile = profile
        self.endpoint_url = endpoint_url
        if credentials:
            os.environ['AWS_SHARED_CREDENTIALS_FILE'] = credentials # 
        self.session=boto3.Session(profile_name=profile)
        self.s3_client = self.session.client('s3', endpoint_url=endpoint_url)

    def __repr__(self):
        return f'S3 Interface: profile {self.profile}; bucket {self.bucket}'

    def exists(self, obj):
        try:
            rec=self.s3_client.head_object(Bucket=self.bucket, Key=obj)
        except: # FIX to be more specific
            return None
        return rec
            
    def moveFile(self, srcFile, destObj, extra={}):
        filesize=os.path.getsize(srcFile)
        self.logger.debug(f"submitting transfer {srcFile}, {destObj}, size {filesize}")
        try:
            self.s3_client.upload_file(Filename=srcFile, Bucket=self.bucket, Key=destObj, ExtraArgs = extra)
        except:
            self.logger.error("upload failed")
    
    def readFile(self, srcObj, destFile, extra={}):
        self.logger.debug(f"submitting transfer {srcObj}, {destFile}")
        try:
            self.s3_client.download_file(Bucket=self.bucket, Key=srcObj, Filename=destFile, ExtraArgs = extra)
        except:
            self.logger.error("upload failed")
    
    def moveFileLike(self, srcFileLike, destObj, extra={}):
        self.logger.debug(f"submitting transfer {srcFileLike}, {destObj}")
        try:
            self.s3_client.upload_fileobj(Fileobj=srcFileLike, Bucket=self.bucket, Key=destObj, ExtraArgs = extra)
        except:
            self.logger.error("upload failed")
    
    def readFileLike(self, srcObj, destFileLike, extra={}):
        self.logger.debug(f"submitting transfer {srcObj}, {destFileLike}")
        try:
            self.s3_client.download_fileobj(Bucket=self.bucket, Key=srcObj, Fileobj=destFileLike, ExtraArgs = extra)
        except:
            self.logger.error("upload failed")
    
    # not used ?
    def getHead(self, obj):
        return self.s3_client.head_object(Bucket=self.bucket, Key=obj)

    def mkDir(self, path): # not needed in S3
        pass

    def listDir(self, dir):
        filelist=[]
        # Initialize paginator
        paginator = self.s3_client.get_paginator('list_objects_v2')

        # Create a PageIterator from the paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=dir)

        # Iterate through each page and print the object keys
        for page in page_iterator:
            if 'Contents' in page:
                filelist.extend([obj['Key'] for obj in page['Contents']])

        return filelist
    

    ''' not used
    def touchObj(self, obj): # for finished file
        self.s3_client.put_object(Bucket=self.bucket, Key=obj)
    '''

    def fatal(self, msg):
        self.logger.error(msg)
        sys.exit(1)

