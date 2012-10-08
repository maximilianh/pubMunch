# exmaple file for pubtools
# searches for sentences that contain two words

import re, doctest

splitter = re.compile(r'[\s{}()]')

searchWords = set(["bcr", "abl"])

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "sentence"]

class D:
    pass

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    """
    >>> d = D()
    >>> d.content = " BCR-ABL "
    >>> list(annotateFile(None, d))
    [[0, 1, "['bcr', 'abl']  BCR-ABL "]]

    go over words of text and check if they are in dict 
    """
    text = file.content
    phrases = text.split(". ")
    for phrase in phrases:
        #yield [phrase]
        #continue
        words = splitter.split(phrase)
        for word in words:
            word = word.lower()
            if "-" in word:
                parts = word.split("-")
            #elif "/" in word:
                #parts = word.split("/")
            else:
                continue

            if len(parts)==2 and parts[0] in searchWords and parts[1] in searchWords:
                yield [0, 1, str(parts)+" "+phrase]

if __name__=="__main__":
    import doctest
    doctest.testmod()
