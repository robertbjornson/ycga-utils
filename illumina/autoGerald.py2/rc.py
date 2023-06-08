import string

complements = string.maketrans('acgtrymkbdhvACGTRYMKBDHV', 'tgcayrkmvhdbTGCAYRKMVHDB')

def rc(dna):
  rcseq = dna.translate(complements)[::-1]
  return rcseq
