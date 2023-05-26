#!/usr/bin/env /home/software/python/Python-2.6.4/bin/python

import re

def oldGaTouchUp(fc):
    return fc+'AAXX'

def defaultTouchUp(fc):
    # strip leading A|B.
    if fc[0] in 'AB': return fc[1:]
    return fc

def nullTouchUp(fc):
    return fc

def guessFlowcell(p):

    pats = [
        # old GA: 100308_FC61DBE_YKAW_CS-1_GA-G
        (r'(?P<punc>[_-])(?P<flowcell>FC.+?)(?P=punc)', oldGaTouchUp),

        # default: 101213_SN515_0098_A811DDABXX
        #(r'\d+_(SN|D)\d+_\d+_(?P<flowcell>.*)$', defaultTouchUp),
        (r'\d+_(SN|D)?\d+R?_\d+_(?P<flowcell>.*)$', defaultTouchUp),

        # miseq: 130131_M01156_0059_000000000-A33GR
        (r'\d+_M\d+_\d+_\d+-(?P<flowcell>.*)$', nullTouchUp),

        # hs4000: 150618_K00162_0009_AH32NWBBXX
        (r'\d+_(K)?\d+R?_\d+_(?P<flowcell>.*)$', defaultTouchUp),

        # NovaSeq:170327_A00124_0014_BH2C52DMXX
        (r'\d+_(A)?\d+R?_\d+_(?P<flowcell>.*)$', defaultTouchUp),
        ]

    for pat, tu in pats:
        m = re.search(pat, p)
        if m:
            return tu(m.group('flowcell'))

    raise Exception('Cannot guess flowcell from path.')

if __name__ == '__main__':
    import sys
    print guessFlowcell(sys.argv[1])
