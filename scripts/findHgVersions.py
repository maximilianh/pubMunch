#import re
onlyMain = True
bestMain = True

headers = ["pmid", "year","version"]
#splitter = re.compile(r"[ ,;.!-()*]")

keywords = ["hg18", "hg19", "hg17", "hg16", "build 36", "build 37", "grch36", "grch37", "reference genome"]

def annotateFile(art, file):
    text = file.content.lower()
    for keyword in keywords:
        if keyword in text:
            yield [art.pmid, art.year, keyword]
            continue
