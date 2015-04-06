headers = ["omeWord"]

bncList = ["some", "home", "come", "come", "home", "come", "become", "income", "become", "become", "outcome", "rome", "welcome", "handsome", "overcome", "welcome", "welcome", "syndrome", "overcome", "welcome", "dome", "unwelcome", "troublesome", "awesome", "frome", "cumbersome", "fearsome", "tiresome", "chromosome", "chromosome", "wholesome", "gruesome", "genome", "epitome", "jerome", "newsome", "some", "overcome", "ayresome", "home", "aerodrome", "nome"]
bncList.append("wellcome")
bncList.append("outcome")
blackList = set(bncList)

def annotateFile(art, file):
    for word in file.content.split():
        word = word.lower()
        #if word.endswith("ome") and word not in blackList:
        if word.endswith("omics") and word not in blackList:
            yield [word]
