from collections import defaultdict

# parse the author names from pubTools format to a simple list of 
# name count lengthOfName

counter = defaultdict(int)
for line in open("authors.txt"):
   names = line.rstrip("\n").split("; ") 
   famNames = [n.split(", ")[0] for n in names]
   for n in famNames:
        if "Consortium" in n or "Project" in n:
            continue
        counter[n] += 1

for name, count in counter.iteritems():
    row = [ name, str(count), str(len(name))]
    print "\t".join(row)
