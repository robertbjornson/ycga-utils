''' utility class to hold various counts'''
class stats(object):
    def __init__(self):
        self.bytes=0
        self.files=0
        self.tarfiles=0
        self.runs=0
        self.deletes=0
        self.errors=0
        
    def comb(self, other):
        self.bytes+=other.bytes
        self.files+=other.files
        self.tarfiles+=other.tarfiles
        self.runs+=other.runs
        self.deletes+=other.deletes
        self.errors+=other.errors


