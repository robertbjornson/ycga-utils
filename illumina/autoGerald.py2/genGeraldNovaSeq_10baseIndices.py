#!/usr/bin/env /home/software/python/Python-2.6.4/bin/python


'''
This is for the NovaSeq, which takes a totally different samplesheet, akin to the one produced by the IEM.  It should look like:

[Header]
[Reads]
[Settings]
[Data]
Lane,SampleID,SampleName,Sample_Plate,Sample_Well,index,index2,Sample_Project,Description,Project
6,Sample_E5553,E5553,na,na,CGATGT,ACATCG,kb374,na,Project_Kb374
6,Sample_E5648,E5648,na,na,TGACCA,TGGTCA,kb374,na,Project_Kb374
6,Sample_E5719,E5719,na,na,GCCAAT,ATTGGC,kb374,na,Project_Kb374
6,Sample_E5723,E5723,na,na,CAGATC,GATCTG,kb374,na,Project_Kb374
6,Sample_E5864,E5864,na,na,CTTGTA,TACAAG,kb374,na,Project_Kb374
6,Sample_E6139,E6139,na,na,CCGTCC,GGACGG,kb374,na,Project_Kb374
7,Sample_E6622,E6622,na,na,CGATGT,ACATCG,kb374,na,Project_Kb374
7,Sample_E4677,E4677,na,na,TGACCA,TGGTCA,kb374,na,Project_Kb374
7,Sample_E5252,E5252,na,na,GCCAAT,ATTGGC,kb374,na,Project_Kb374
7,Sample_E6723,E6723,na,na,CAGATC,GATCTG,kb374,na,Project_Kb374
7,Sample_SLA1272,SLA1272,na,na,CTTGTA,TACAAG,kb374,na,Project_Kb374
7,Sample_SLA1276,SLA1276,na,na,CCGTCC,GGACGG,kb374,na,Project_Kb374

'''

import re, os, StringIO, sys, urllib2, rc
from guessFlowcell import guessFlowcell
from lxml import etree
from os import environ as Env
from os.path import exists as PE, join as PJ, split as PS
import csv

def parseBCs(fn):
    d={}
    with open(fn) as csvfile:
        r=csv.reader(csvfile)
        hdr=r.next()
        for idx, bs in r:
            d[idx]=bs
    return d

# NEED FIX FOR CASAVA 1.8 => 1.8 squashes on the fly. Need to ensure the genome directory contains only those .fa files that, taken together, constitute a (non-redundant?) genome.
abbreviations = {
    'ELAND_GENOME' : {

    'brainseq_h19_sq': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/brainseq/dna/sam_brainseq_h19/sam_brainseq_h19.fa',
            'MissingAnnotation',
            'MissingAbundant',
            ),
    
    'CoxBurn_sq_tidy': (
            '/home/bioinfo/genomes/illumina/squashed/current/CoxiellaBurnetii/genome/c1.8_CoxBurn_sq_tidy',
            'MissingAnotation',
            'MissingAbundant',
            ),

    'galGal3_tidy': (
            '/home/bioinfo/genomes/illumina/squashed/current/galGal3/genome/c1.8_galGal3_sq_tidy',
            '/home/bioinfo/genomes/illumina/squashed/current/galGal3/refFlat.txt.gz',
            '/home/bioinfo/genomes/illumina/squashed/current/galGal3/genome/c1.8_galGal3_abundant',
            ),

    'hg18_sq_tidy': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg18/genome/c1.8_hg18_sq_tidy',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg18/refFlat.txt.gz',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg18/genome/c1.8_hg18_abundant',
            ),

    'hg19_sq_tidy': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg19/genome/c1.8_hg19_sq_tidy',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg19/refFlat.txt.gz',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/hg19/genome/c1.8_hg19_abundant',
            ),

    'mm9_sq_tidy': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/mm9/genome/c1.8_mm9_sq_tidy',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/mm9/refFlat.txt.gz',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/mm9/genome/c1.8_mm9_abundant',
            ),

    'oriSat12_sq_tidy':	(
            '/home/bioinfo/genomes/illumina/squashed/current/OryzaSativa12/genome/c1.8_oryzSat12_sq_tidy',
            'MissingAnotation',
            'MissingAbundant',
            ),

    'panTro2_sq_tidy': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/panTro2/genome/c1.8_panTro2_sq_tidy',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/panTro2/refFlat.txt.gz',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/panTro2/genome/c1.8_panTro2_abundant',
            ),

    'PhiX174_sq': (
            'NEED FIX FOR CASAVA 1.8 /home/bioinfo/genomes/illumina/squashed/current/PhiX174/PhiX174_sq',
            'MissingAnnotation',
            'MissingAbundant',
            ),

    'rheMac2_sq': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/rheMac2-100428/genome/c1.8_rheMac2_sq_tidy',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/rheMac2-100428/refFlat.txt.gz',
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/rheMac2-100428/genome/c1.8_ucsc_rheMac2_abundant',
            ),

    'S.pneumoniae_TIGR4_sq': (
            'NEED FIX FOR CASAVA 1.8 /home/bioinfo/genomes/illumina/squashed/current/S.pneumoniae_TIGR4/genome/S.pneumoniae_TIGR4_sq',
            'MissingAnnotation',
            'MissingAbundant',
            ),

    'sorgBicolor12_sq_tidy': (
            '/home/bioinfo/genomes/illumina/squashed/current/SorghumBicolor12/genome/c1.8_sorgBic12_sq_tidy',
            'MissingAnotation',
            'MissingAbundant',
            ),

    'Salmonella_Typhimurium_14028S': (
            '/panfs/home/bioinfo/genomes/illumina/squashed/current/SalmonellaTyphimurium_14028S/genome/c1.8_sTyphimurium_14028S_sq_tidy',
            'MissingAnnotation',
            'MissingAbundant',
            ),

    'TBruce_sq': (
            'NEED FIX FOR CASAVA 1.8 /home/bioinfo/genomes/illumina/squashed/current/TrypanosomaBrucei/genome/TBruce_sq',
            'MissingAnnotation',
            'MissingAbundant',
            ),

    'xanthomonas_axonopodis_sq': (
            'NEED FIX FOR CASAVA 1.8 /home/bioinfo/genomes/illumina/squashed/current/xanthomonas_axonopodis/genome/xanthomonas_axonopodis_sq',
            'MissingAnnotation',
            'MissingAbundant',
            ),

    'zeaMays12_sq_tidy': (
            '/home/bioinfo/genomes/illumina/squashed/current/ZeaMays12/genome/c1.8_zeaMays12_sq_tidy',
            'MissingAnotation',
            'MissingAbundant',
            ),

    }
}

isSamtoolsGenome = {
    'brainseq_h19_sq': True,
}

# This script uses the new 10 base indices, which are read from a file
index2barcode=parseBCs('/gpfs/ycga/home/rob/projects/autoGerald/UDI_Indexes.csv')

#bcre = re.compile('(?:index )?(\d+)(?: *- *(?:index )?(\d+))?', re.IGNORECASE)
# changed to reflect preprocessing removal of spaces
bcre = re.compile('(?:index)?(\d+)(?:-(?:index)?(\d+))?', re.IGNORECASE)
badbcx = 'NOBCX'

# Make a name file system compatible.
def cleanName(n):
    rn = []
    for c in n:
        if not c.isalnum() and not c in '_-':
            if c.isspace():
                c = '_'
            else:
                c = '_%02x_'%ord(c)
        rn.append(c)
    return ''.join(rn)

runFolderPath = sys.argv[1]
# a bit of a hack, but using completion it's easy to end up with a trailing '/'
if runFolderPath[-1] == '/': runFolderPath = runFolderPath[:-1]

runFolder = os.path.split(runFolderPath)[1]

# figure out what sort of run this is: single/pair?, multiplexed?
rit = etree.parse(open(runFolderPath+'/RunInfo.xml'))
multiplexed, readArity = False, 0
for rrr in rit.xpath('Run/Reads/Read'):
    if rrr.attrib['IsIndexedRead'] == 'Y':
        multiplexed = True
    else:
        readArity += 1

fc =  guessFlowcell(runFolder)

# ask the twisted server that uses python to access wikilims data for info about this flowcell
url = 'http://wikilims.ycga.yale.edu:6789/dumpAnalysis.rpy?target=%s'%fc
#url = 'http://sysg1.cs.yale.edu:6789/dumpAnalysis.rpy?target=%s'%fc
print >> sys.stderr, '>>> TEMPORARY URL CHANGE. REMIND NJC TO RESET THIS WHEN WIKILIMS IS BACK UP. <<<'
print >> sys.stderr, url

reply = urllib2.urlopen(url).read()
if reply.startswith('Bummer:'):
    print reply
    sys.exit(1)

sampleInfo = eval(reply)
# ('5', 'TRIP-N', 'Index 12', 'User:Rhalaban', 'eland_pair', 'nY74n,nY74n', 'Hg18', 'hg18_sq_tidy')
cols = dict([(l, x) for x, l in enumerate(['Sample', 'Lane', 'Name', 'BCX', 'Owner', 'Analysis', 'UB', 'Genome', 'GenomeDir'])])

email, pre, post = Env.get('AUTOGER_EMAIL', None), Env.get('AUTOGER_GPRE', None), Env.get('AUTOGER_GPOST', None)

geraldCommon = ''

if email:
    geraldCommon = ''' \
EMAIL_LIST %s, email
EMAIL_SERVER mail.yale.edu
EMAIL_DOMAIN yale.edu\n
'''%(email,)

# TODO: find a better way to determine machine type.
# Value per CASAVA 1.8 Docs. Appears to be no longer dependent on sequencer technology.
if rit.xpath('Run/Instrument')[0].text.startswith('GA-'):
    geraldCommon += 'ELAND_FASTQ_FILES_PER_PROCESS 3\n\n'
else:
    geraldCommon += 'ELAND_FASTQ_FILES_PER_PROCESS 3\n\n'

# placed here so that there can be per lane overrides.
# coding it this way raises an exception if the count is not 1 or 2.
geraldCommon += {1: 'USE_BASES nY*n', 2: 'USE_BASES nY*n,nY*n'}[readArity] + '\n\n'

csv = open('generatedSampleSheet.csv', 'w')
# is the header line really necessary?
print >> csv, '''[Header]
[Reads]
[Settings]
[Data]
Lane,SampleID,SampleName,Sample_Plate,Sample_Well,index,index2,Sample_Project,Description,Project'''

#print >>csv, 'FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,Project'

template = open('generatedGeraldTemplate.txt', 'w')
template.write('#\n#\n') # work around a bug in illumina's code (getEOL in Utils.pm).
template.write(geraldCommon)

for s in sorted(sampleInfo):
    sample, analysis, bcx, lane, name, owner, ub, gdir = [s[cols[l]] for l in ['Sample', 'Analysis', 'BCX', 'Lane', 'Name', 'Owner', 'UB', 'GenomeDir']]
    #print >>sys.stderr, sample+':\t'+'\t'.join([analysis, bcx, lane, name, owner, ub, gdir])
    sampleText = '[' + ','.join(['"%s"'%e for e in [sample, analysis, bcx, lane, name, owner, ub, gdir]]) + ']'
    if analysis.startswith('sequence'):
        print >>sys.stderr, 'WARNING: analysis "%s" replaced by "none" for "%s".'%(analysis,sampleText)
        analysis = 'none'
    name = cleanName(name)
    if owner.startswith('User:'): owner = owner[5:]
    owner = cleanName(owner)
 
    # as of 120109, legitimate barcode names are 'Index 1', ... 'Index 12' (see above), but allow for numbers only.
    # grrrr --- and lower case!
    subSamples = bcx.replace(' ', '').split(',')
    tNames = {}
    for ssx, bcd in enumerate(subSamples):
        
        m = bcre.match(bcd)
        if not m:
            print >>sys.stderr, 'WARNING: invalid barcode for %s, setting index to "%s".'%(sampleText,badbcx)
            bcx, bcseq = badbcx, ''
        else:
            x1, x2 = m.groups()
            if x2:
                if x1 not in index2barcode or x2 not in index2barcode:
                    print >>sys.stderr, 'WARNING: bad dual barcode for %s: "Index %s - Index %s". Ssetting index to "%s".'%(sampleText,x1,x2,badbcx)
                    bcx, bcseq = badbcx, ''
                else:
                    bcx, bcseq = '%03d_%03d'%(int(x1), int(x2)), index2barcode[x1] + "," + index2barcode[x2]
            else:
                if x1 not in index2barcode:
                    print >>sys.stderr, 'WARNING: bad barcode for %s: "Index %s". Ssetting index to "%s".'%(sampleText,x1,badbcx)
                    bcx, bcseq = badbcx, ''
                else:
                    bcx, bcseq = '%03d'%int(x1), index2barcode[x1]
                
        refName = '%s_%s'%(name, lane)

        tName = name + '_' + bcx
        tnc = tNames.get(tName, 0)
        tNames[tName] = tnc + 1
        if tnc:
            nTName = '%s_%02d'%(tName, tnc+1)
            print >>sys.stderr, 'WARNING: duplicate extended Sample ID name "%s" for "%s", changing to "%s".'%(tName,sampleText,nTName)
            tName = nTName

        #if len(subSamples) > 1: tName = '%s_%03d'%(name, ssx+1)
        print >> csv, ','.join([lane, 'Sample_'+tName, tName, 'na', 'na', bcseq, owner, sample, 'Project_'+owner])
        #print >>csv, ','.join([fc, lane, tName, refName, bcseq, 'NoDescription', 'N', 'StoneSoup', 'Noman', owner])

    print >>template, 'REFERENCE %s ANALYSIS %s'%(refName, analysis)
    if ub != '???': print >>template, 'REFERENCE %s USE_BASES %s'%(refName, ub)
    if 'eland' in analysis:
        genomeDir, annotationFile, abundantDir = abbreviations['ELAND_GENOME'].get(gdir, (gdir, PJ(PS(gdir)[0], 'refFlat.txt.gz'), PJ(PS(gdir)[0], '???')))
        if not PE(genomeDir):
            print >>sys.stderr, 'WARNING: ELAND_GENOME "%s" does not exist for %s.'%(genomeDir, sampleText)
        eg = 'ELAND_GENOME'
        if gdir in isSamtoolsGenome: eg = 'SAMTOOLS_GENOME'
        print >>template, 'REFERENCE %s %s %s'%(refName, eg, genomeDir)
        if 'eland_rna' == analysis:
            if not PE(annotationFile):
                print >>sys.stderr, 'WARNING: ELAND_RNA_GENOME_ANNOTATION "%s" does not exist for %s.'%(annotationFile, sampleText)
            print >>template, 'REFERENCE %s ELAND_RNA_GENOME_ANNOTATION %s'%(refName, annotationFile)
            if not PE(abundantDir):
                print >>sys.stderr, 'WARNING: ELAND_RNA_GENOME_CONTAM "%s" does not exist for %s.'%(abundantDir, sampleText)
            print >>template, 'REFERENCE %s ELAND_RNA_GENOME_CONTAM %s'%(refName, abundantDir)


csv.close()
template.close()

if pre: sys.stdout.write(open(pre).read())

if post: sys.stdout.write(open(post).read())
