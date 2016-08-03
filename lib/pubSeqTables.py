# the basic codon tables

# long form -> one-letter conversion table for amino acids
threeToOne = \
    {'Cys': 'C', 'Asp': 'D', 'Ser': 'S', 'Gln': 'Q', 'Lys': 'K',
     'Ile': 'I', 'Pro': 'P', 'Thr': 'T', 'Phe': 'F', 'Asn': 'N',
     'Gly': 'G', 'His': 'H', 'Leu': 'L', 'Arg': 'R', 'Trp': 'W',
     'Ala': 'A', 'Val':'V',  'Glu': 'E', 'Tyr': 'Y', 'Met': 'M',
     'Sec': 'U',  # Sec = sometimes used as amino acid (same as Ter)
     'Ter': '*',   # the termination codon
     'Stop': '*',   # the termination codon
     'Fs': '*',   # the termination codon
     'X': '*',   # the termination codon
     'Alanine'       : 'A',
     'Asparagine'    : 'B',
     'Cysteine'      : 'C',
     'Aspartate'     : 'D',
     'Aspartic Acid' : 'D',
     'Glutamic Acid' : 'E',
     'Glutamate'     : 'E',
     'Phenylalanine' : 'F',
     'Glycine'       : 'G',
     'Histidine'     : 'H',
     'Isoleucine'    : 'I',
     'Lysine'        : 'K',
     'Leucine'       : 'L',
     'Methionine'    : 'M',
     'Asparagine'    : 'N',
     'Proline'       : 'P',
     'Glutamine'     : 'Q',
     'Arginine'      : 'R',
     'Serine'        : 'S',
     'Threonine'     : 'T',
     'Valine'        : 'V',
     'Tryptophan'    : 'W',
     'Any'           : 'X',
     'Tyrosine'      : 'Y',
     }

# same but lower case
threeToOneLower = dict([[k.lower(),v] for k,v in threeToOne.items()])
# one-letter -> three-letter conversion table for amino acids
oneToThree = \
    {'C':'Cys', 'D':'Asp', 'S':'Ser', 'Q':'Gln', 'K':'Lys',
     'I':'Ile', 'P':'Pro', 'T':'Thr', 'F':'Phe', 'N':'Asn',
     'G':'Gly', 'H':'His', 'L':'Leu', 'R':'Arg', 'W':'Trp',
     'A':'Ala', 'V':'Val', 'E':'Glu', 'Y':'Tyr', 'M':'Met',
     'U':'Sec', '*':'Stop',
     'X':'Stop',  # is this really used like that?
     'Z':'Glx', # special case: asparagine or aspartic acid
     'B':'Asx'  # special case: glutamine or glutamic acid
     }

# from amino acid to all possible codons
aaToDna = {
 'A': ['GCA', 'GCC', 'GCG', 'GCT'],
 'C': ['TGT', 'TGC'],
 'D': ['GAT', 'GAC'],
 'E': ['GAG', 'GAA'],
 'F': ['TTT', 'TTC'],
 'G': ['GGT', 'GGG', 'GGA', 'GGC'],
 'H': ['CAT', 'CAC'],
 'I': ['ATC', 'ATA', 'ATT'],
 'K': ['AAG', 'AAA'],
 'L': ['CTC', 'CTG', 'CTA', 'CTT', 'TTA', 'TTG'],
 'M': ['ATG'],
 'N': ['AAC', 'AAT'],
 'P': ['CCT', 'CCA', 'CCG', 'CCC'],
 'Q': ['CAG', 'CAA'],
 'R': ['AGG', 'AGA', 'CGA', 'CGC', 'CGG', 'CGT'],
 'S': ['AGC', 'AGT', 'TCG', 'TCA', 'TCC', 'TCT'],
 'T': ['ACC', 'ACA', 'ACG', 'ACT'],
 'V': ['GTA', 'GTC', 'GTG', 'GTT'],
 'W': ['TGG'],
 'Y': ['TAT', 'TAC'],
 '*': ['TAG', 'TAA', 'TGA'],
 'X': ['TAG', 'TAA', 'TGA']
 }

# the genetic code
dnaToAa = {
    'ATA':'I', 'ATC':'I', 'ATT':'I', 'ATG':'M',
    'ACA':'T', 'ACC':'T', 'ACG':'T', 'ACT':'T',
    'AAC':'N', 'AAT':'N', 'AAA':'K', 'AAG':'K',
    'AGC':'S', 'AGT':'S', 'AGA':'R', 'AGG':'R',
    'CTA':'L', 'CTC':'L', 'CTG':'L', 'CTT':'L', 
    'CTN':'L',
    'CCA':'P', 'CCC':'P', 'CCG':'P', 'CCT':'P',
    'CCN':'P',
    'CAC':'H', 'CAT':'H', 'CAA':'Q', 'CAG':'Q',
    'CGA':'R', 'CGC':'R', 'CGG':'R', 'CGT':'R',
    'CGN':'R',
    'GTA':'V', 'GTC':'V', 'GTG':'V', 'GTT':'V',
    'GTN':'V',
    'GCA':'A', 'GCC':'A', 'GCG':'A', 'GCT':'A',
    'GCN':'A',
    'GAC':'D', 'GAT':'D', 'GAA':'E', 'GAG':'E',
    'GGA':'G', 'GGC':'G', 'GGG':'G', 'GGT':'G',
    'GGN':'G',
    'TCA':'S', 'TCC':'S', 'TCG':'S', 'TCT':'S',
    'TCN':'S',
    'TTC':'F', 'TTT':'F', 'TTA':'L', 'TTG':'L',
    'TAC':'Y', 'TAT':'Y', 'TAA':'_', 'TAG':'_',
    'TGC':'C', 'TGT':'C', 'TGA':'_', 'TGG':'W',
    }


