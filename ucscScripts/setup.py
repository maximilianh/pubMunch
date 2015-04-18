import sys
from cx_Freeze import setup, Executable

myPath = sys.path
myPath.append("lib")

# Dependencies are automatically detected, but it might need fine tuning.
#"include_files" : ["../curlWin64/bin/curl.exe"],

build_exe_options = {"packages": ["os"],
 "excludes": ["tkinter"],
	"path": myPath
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
        executables = [Executable("pubCrawl2", base=base)],
)
