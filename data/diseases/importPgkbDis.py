from __future__ import print_function
for line in open("diseases.tsv"):
    if line.startswith("PharmGKB"):
        continue
    fields = line.split("\t")
    name = fields[1]
    syns = fields[2].split('","')
    syns = [s.strip('",') for s in syns]
    print("%s\t%s" % (name, "|".join(syns)))

