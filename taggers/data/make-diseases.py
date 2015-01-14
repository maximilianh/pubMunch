import gzip

ofh = gzip.open("diseases.tab.gz", "w")
terms = set()
# MESH TERMS
# Body Regions;A01
# WAGR Syndrome;C04.700.635.950
# WAGR Syndrome;C10.597.606.643.969
lines = open("rawData/mtrees2013.bin")
syns = []
name = None
for line in lines:
    line = line.strip()
    term, code = line.split(";")
    term = term.strip()
    if code.startswith("C"):
        terms.add(term)
        syns = []

# HUMAN DISEASE ONTOLOGY
# name: gallbladder disease
# synonym: "Hemangioendothelioma, malignant (morphologic abnormality)" EXACT [SNOMEDCT_2005_07_31:33176006]
lines = open("rawData/HumanDO.obo")
for line in lines:
    line = line.strip()
    fs = line.split()
    if line.startswith("name: "):
        terms.add(" ".join(fs[1:]))
    if line.startswith("synonym: "):
        dis = line.split('"')[1]
        dis = dis.split("(")[0].strip().split("[")[0].strip()
        terms.add(dis)

# SNOMED CORE
#38341003|Hypertensive disorder, systemic arterial (disorder)|Current|C0020538|8|3.2242|200907|False||
for line in open("rawData/SNOMEDCT_CORE_SUBSET_201211.txt"):
    if line.startswith("SNOMED"):
        continue
    if "(disorder)" not in line:
        continue
    fs = line.split("|")
    dis = fs[1]
    dis = dis.split("(")[0].strip()
    term = dis.replace(", disease","")
    terms.add(term)

# basic postprocessing
lowTerms = set()
for term in terms:
    if len(term)<=4:
        continue
    term.strip()
    lowTerms.add(term.lower())

lowTerms.remove("disease")
lowTerms.remove("syndrome")
lowTerms.remove("isolated")

for term in lowTerms:
    term = term.strip()
    ofh.write(term+"\n")
print "wrote %d terms to to %s" % (len(terms), ofh.name)
