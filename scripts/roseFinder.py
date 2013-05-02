# annotator to look for certain protein interaction keywords
import re

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["protein", "keyword", "phrase"]

onlyMain = True
bestMain = True

proteinIds = {}
geneIds = {}
def startup(paramDict):
    fname = "/cluster/home/max/projects/pubs/analysis/roseOughtred/roseNames.tab"
    lines = open(fname).read().splitlines()
    for line in lines:
        id, protName, geneNames = line.split("\t")
        if protName!="":
            proteinIds[protName.lower()] = id
        if geneNames!="":
            geneNames = geneNames.split("|")
            for geneName in geneNames:
                geneIds[geneName] = id
    
keywords = ["interact "," interacts "," interactor "," interacts "," interaction "," interactions "," interacted "," bind "," binds "," binding "," bound "," complexed "," contact "," contacts "," contacted "," coimmunoprecipitated "," coimmunoprecipitates "," coimmunoprecipitations "," coimmunoprecipitation "," coimmunoprecipitate "," coprecipitation "," coprecipitations "," coprecipitates "," coprecipitate "," immunoprecipitation "," immunoprecipitated "," immunoprecipitate "," immunoprecipitations "," immunoprecipitates "," co-immunoprecipitated "," co-immunoprecipitates "," co-immunoprecipitations "," co-immunoprecipitation "," co-immunoprecipitate "," co-precipitation "," co-precipitations "," co-precipitates "," co-precipitate "," two hybrid "," two-hybrid "," co-localize "," co-localization "," co-localized "," co-localizes "," colocalize "," colocalization "," colocalized "," colocalizes "," converted "," converts "," convert "," converting "," modify "," modifies "," modified "," modifier "," modifiers "," modifying "," modify "," modifies "," modified "," modifier "," modifiers "," modifying "," coassembled "," coassembly "," coassemblies "," coassemble "," coassemblies "," co-assembled "," co-assembly "," coassemblies "," co-assemble "," co-assemblies"]

def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    if file.fileType=="supp":
        return
    for phrase in re.split("[.;:] ", text):
        if len(phrase)>1000:
            continue
        phraseLow = phrase.lower()
        foundProt = None
        foundProtName = None
        # look for protein name
        for protName, protId in proteinIds.iteritems():
            if protName in phraseLow:
                foundProt = protId
                foundProtName = protName
                break
        # look for gene name
        for geneName, protId in geneIds.iteritems():
            phraseWords = set(re.split("[, ;!.-]", phrase))
            if geneName in phraseWords:
                foundProt = protId
                foundProtName = geneName
                break
        if foundProt==None:
            continue

        # look for keyword
        foundKw = None
        for kw in keywords:
            if kw in phraseLow:
                foundKw = kw
                break
        if foundKw==None:
            continue
        yield [foundProt, foundProtName, foundKw, phrase]


                

