# an annotator that is returning the first word of every file and creates a new 
# output file for every result

headers = ['firstWord']

def annotateFile(article, file):
    words = file.content.split()
    if len(words)>2:
        yield [words[0]]
        yield [] # = "open new output file"
        yield [words[1]]
