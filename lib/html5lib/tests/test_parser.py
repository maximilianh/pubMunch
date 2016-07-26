import os
import sys
import traceback
import io
import warnings
import re

warnings.simplefilter("error")

from .support import html5lib_test_files as data_files
from .support import TestData, convert, convertExpected
import html5lib
from html5lib import html5parser, treebuilders, constants

treeTypes = {"simpletree":treebuilders.getTreeBuilder("simpletree"),
             "DOM":treebuilders.getTreeBuilder("dom")}

#Try whatever etree implementations are avaliable from a list that are
#"supposed" to work
try:
    import xml.etree.ElementTree as ElementTree
    treeTypes['ElementTree'] = treebuilders.getTreeBuilder("etree", ElementTree, fullTree=True)
except ImportError:
    try:
        import elementtree.ElementTree as ElementTree
        treeTypes['ElementTree'] = treebuilders.getTreeBuilder("etree", ElementTree, fullTree=True)
    except ImportError:
        pass

try:
    import xml.etree.cElementTree as cElementTree
    treeTypes['cElementTree'] = treebuilders.getTreeBuilder("etree", cElementTree, fullTree=True)
except ImportError:
    try:
        import cElementTree
        treeTypes['cElementTree'] = treebuilders.getTreeBuilder("etree", cElementTree, fullTree=True)
    except ImportError:
        pass
    
try:
    try:
        import lxml.html as lxml
    except ImportError:
        import lxml.etree as lxml
    treeTypes['lxml'] = treebuilders.getTreeBuilder("lxml", lxml, fullTree=True)
except ImportError:
    pass

try:
    import BeautifulSoup
    treeTypes["beautifulsoup"] = treebuilders.getTreeBuilder("beautifulsoup", fullTree=True)
except ImportError:
    pass

#Try whatever dom implementations are avaliable from a list that are
#"supposed" to work
try:
    import pxdom
    treeTypes["pxdom"] = treebuilders.getTreeBuilder("dom", pxdom)
except ImportError:
    pass

#Run the parse error checks
checkParseErrors = False

#XXX - There should just be one function here but for some reason the testcase
#format differs from the treedump format by a single space character
def convertTreeDump(data):
    return "\n".join(convert(3)(data).split("\n")[1:])

namespaceExpected = re.compile(r"^(\s*)<(\S+)>", re.M).sub


def runParserTest(innerHTML, input, expected, errors, treeClass,
                  namespaceHTMLElements):
    #XXX - move this out into the setup function
    #concatenate all consecutive character tokens into a single token
    try:
        p = html5parser.HTMLParser(tree = treeClass,
                                   namespaceHTMLElements=namespaceHTMLElements)
    except constants.DataLossWarning:
        return

    try:
        if innerHTML:
            document = p.parseFragment(input, innerHTML)
        else:
            try:
                document = p.parse(input)
            except constants.DataLossWarning:
                return 
    except:
        errorMsg = "\n".join(["\n\nInput:", input, "\nExpected:", expected,
                               "\nTraceback:", traceback.format_exc()])
        assert False, errorMsg.encode("utf8")

    output = convertTreeDump(p.tree.testSerializer(document))

    expected = convertExpected(expected)
    if namespaceHTMLElements:
        expected = namespaceExpected(r"\1<html \2>", expected)

    errorMsg = "\n".join(["\n\nInput:", input, "\nExpected:", expected,
                           "\nReceived:", output])
    assert expected == output, errorMsg.encode("utf8")
    errStr = ["Line: %i Col: %i %s"%(line, col, 
                                      constants.E[errorcode] % datavars if isinstance(datavars, dict) else (datavars,)) for
              ((line,col), errorcode, datavars) in p.errors]

    errorMsg2 = "\n".join(["\n\nInput:", input,
                            "\nExpected errors (" + str(len(errors)) + "):\n" + "\n".join(errors),
                            "\nActual errors (" + str(len(p.errors)) + "):\n" + "\n".join(errStr)])
    if checkParseErrors:
            assert len(p.errors) == len(errors), errorMsg2.encode("utf-8")

def test_parser():
    sys.stderr.write('Testing tree builders '+ " ".join(list(treeTypes.keys())) + "\n")
    files = data_files('tree-construction')
    
    for filename in files:
        testName = os.path.basename(filename).replace(".dat","")

        tests = TestData(filename, "data")
        
        for index, test in enumerate(tests):
            input, errors, innerHTML, expected = [test[key] for key in
                                                      ('data', 'errors',
                                                      'document-fragment',
                                                      'document')]
            if errors:
                errors = errors.split("\n")

            for treeName, treeCls in treeTypes.items():
                for namespaceHTMLElements in (True, False):
                    print(input)
                    yield (runParserTest, innerHTML, input, expected, errors, treeCls,
                           namespaceHTMLElements)
                    break
                
                
