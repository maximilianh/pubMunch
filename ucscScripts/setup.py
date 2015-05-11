import sys, glob, os
from cx_Freeze import setup, Executable

myPath = sys.path
myPath.append("lib")

# Dependencies are automatically detected, but it might need fine tuning.

# include all modules, as they might get loaded from the taggers during runtime
allPkgs = ["os"]
for modName in os.listdir("lib"):
    if modName.endswith(".py"):
        allPkgs.append(modName.split(".")[0])

build_exe_options = {"packages": allPkgs,
 "excludes": ["tkinter"],
	"path": myPath,
"include_files" : ["taggers/", "data/"]
}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
#if sys.platform == "win32":
    #base = "Win32GUI"

setup(  name = "pubCrawl",
        version = "0.1",
        description = "My GUI application!",
        options = {"build_exe": build_exe_options},
        executables = [Executable("pubCrawl2", base=base),\
            Executable("pubRunAnnot", base=base)],
)
