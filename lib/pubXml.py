import sys, logging, traceback, re

# load lxml parser
try:
    from lxml import etree # you can install this. Debian/Redhat package: python-lxml, see also: codespeak.net/lxml/installation.html
    lxmlLoaded=True
except ImportError:
    import xml.etree.cElementTree as etree # this is the slower, python2.5 default package
    lxmlLoaded=False

def etreeFromXml(string):
    """ parses string to etree and removes all namespaces """
    global lxmlLoaded
    if lxmlLoaded:
        tree   = etree.fromstring(string).getroottree().getroot()
    else:
        tree = etree.fromstring(string)
        #print dir(tree)
        #tree = tree.getroot()
    strip_namespace_inplace(tree)
    return tree

def treeToAsciiText(tree, _addtail=False, addNewlineTags=None):
    textList = recursiveToAscii(tree, addNewlineTags=addNewlineTags)
    return "".join(textList)

def recursiveToAscii(tree, _addtail=True, addNewlineTags=None):
    """ xml -> ascii tags: convert all text associated with all tags to a
    space-sep. ASCII text string in utf8
    copied from http://code.activestate.com/recipes/498286/
    Remove all tabstops.
    Returns a list of text strings contained within an element and its sub-elements.
    Helpful for extracting text from prose-oriented XML (such as XHTML or DocBook).

    Add a \n whenever one of the tags in addNewlineTags is found.
    """
    result = []
    if tree.tag!=None and addNewlineTags!=None:
        if tree.tag in addNewlineTags:
            result.append("\n")
    if tree.text is not None:
        result.append(" ")
        result.append(tree.text.replace("\t", "").replace("\n", "").strip())
    for elem in tree:
        result.extend(recursiveToAscii(elem,True, addNewlineTags))
    if _addtail and tree.tail is not None:
        result.append(" ")
        result.append(tree.tail.replace("\t","").replace("\n", "").strip())
    return result

def pmcCleanXmlStr(xmlStr):
    """
    substitute some common PMC-xml elements with normal html that makes more sense

    >>> pmcCleanXmlStr("<abstract namespace=nonsense>Hi there</abstract>")
    'Hi there'

    """
    xmlStr = xmlStr.replace("<sec>","<p>")
    xmlStr = xmlStr.replace("</sec>","</p>")
    xmlStr = xmlStr.replace("<title>","<b>")
    xmlStr = xmlStr.replace("</title>","</b>")
    xmlStr = xmlStr.replace("<italic>","<i>")
    xmlStr = xmlStr.replace("</italic>","</i>")
    xmlStr = xmlStr.replace("<bold>","<b>")
    xmlStr = xmlStr.replace("</bold>","</b>")
    xmlStr = xmlStr.replace("<abstract>","")
    #xmlStr = xmlStr.replace('<abstract xmlns:xlink="http://www.w3.org/1999/xlink">',"") # bad hack
    xmlStr = re.sub(r'<abstract [^>]+>', '', xmlStr) # another bad hack
    xmlStr = xmlStr.replace("</abstract>","")
    return xmlStr

def pmcAbstractToHtml(element):
    """ substitute some common PMC-xml elements with normal html that make sense

    """
    xmlStr = etree.tostring(element)
    return pmcCleanXmlStr(xmlStr)

def strip_namespace_inplace(etree, namespace=None,remove_from_attr=True):
    """ Takes a parsed ET structure and does an in-place removal of all namespaces,
        or removes a specific namespacem (by its URL).

        Can make node searches simpler in structures with unpredictable namespaces
        and in content given to be non-mixed.

        By default does so for node names as well as attribute names.
        (doesn't remove the namespace definitions, but apparently
         ElementTree serialization omits any that are unused)

        Note that for attributes that are unique only because of namespace,
        this may attributes to be overwritten.
        For example: <e p:at="bar" at="quu">   would become: <e at="bar">

        I don't think I've seen any XML where this matters, though.
    """
    if namespace==None: # all namespaces
        for elem in etree.getiterator():
            tagname = elem.tag
            if not isinstance(elem.tag, basestring):
                continue
            if tagname[0]=='{':
                elem.tag = tagname[ tagname.index('}',1)+1:]

            if remove_from_attr:
                to_delete=[]
                to_set={}
                for attr_name in elem.attrib:
                    if attr_name[0]=='{':
                        old_val = elem.attrib[attr_name]
                        to_delete.append(attr_name)
                        attr_name = attr_name[attr_name.index('}',1)+1:]
                        to_set[attr_name] = old_val
                for key in to_delete:
                    elem.attrib.pop(key)
                elem.attrib.update(to_set)

    else: # asked to remove specific namespace.
        ns = '{%s}' % namespace
        nsl = len(ns)
        for elem in etree.getiterator():
            if elem.tag.startswith(ns):
                elem.tag = elem.tag[nsl:]

            if remove_from_attr:
                to_delete=[]
                to_set={}
                for attr_name in elem.attrib:
                    if attr_name.startswith(ns):
                        old_val = elem.attrib[attr_name]
                        to_delete.append(attr_name)
                        attr_name = attr_name[nsl:]
                        to_set[attr_name] = old_val
                for key in to_delete:
                    elem.attrib.pop(key)
                elem.attrib.update(to_set)

def nxmlHasBody(inData):
    """ try to find out if a PMC xml file has some text in it or if
        it's just scanned pages """
    #xml  = codecs.open(nxmlName, encoding="utf8").read()
    try:
        root = etreeFromXml(inData)
        body = findChild(root, "body")
        scans = findChildren(body,"supplementary-material", reqAttrName="content-type", reqAttrValue='scanned-pages')
        if body!=None and len(scans)==0:
            logging.debug("Found body tag, no scanned pages within it, seems to contain normal fulltext")
            return True
        else:
            logging.debug("No body tag or only scanned pages: No fulltext")
            return False
    except IOError:
        logging.error("IOError while searching for body tag in xml file")
        return False

def stripXmlTags(inData, isNxmlFormat=False, isElsevier=False):
    """ read inFile, strip all XML tags, and return as string"""

    # do not process PMC files without a body tag
    if isNxmlFormat and not nxmlHasBody(inData):
        return None

    try:
        root = etreeFromXml(inData)
        if isElsevier:
            asciiData = treeToAscii_Elsevier(root)
        #elif isNxmlFormat:
            #pmcTags = set(["title","sec","p","section","caption","label","table"])
            #asciiData = treeToAsciiText(root, addNewlineTags=pmcTags)
        # it doesn't hurt to always try the PMC tags -> \n replacement
        else:
            pmcTags = set(["title","sec","p","section","caption","label","table"])
            asciiData = treeToAsciiText(root, addNewlineTags=pmcTags)
        return asciiData

    except SyntaxError:
        logging.error("Error while converting xml to text")
        exObj, exMsg, exTrace = sys.exc_info()
        logging.error("Exception %s, traceback: %s" % (exMsg, traceback.format_exc(exTrace)))
        return None

def findChild(tree, path, convertToAscii=False, reqAttrName=None, reqAttrValue=None, squeak=True):
    """ return a tree element, find child with given path, and optional attributes """
    elements = findChildren(tree, path, convertToAscii, reqAttrName, reqAttrValue)
    #print tree, path, elements
    if len(elements)==1:
        return elements[0]
    elif len(elements)==0:
        if squeak:
            logging.warn("path %s not found" % path)
        return None
    else:
        if squeak:
            logging.warn("path %s lead to more than one value, using only first one" % path)
        return elements[0]

def findChildren(tree, path, convertToAscii=False, reqAttrName=None, reqAttrValue=None):
    """ return all matching tree elements, find child with given path, and optional attributes """
    if tree==None:
        return []

    elements = tree.findall(path)

    if elements==None or len(elements)==0:
        return []

    # after bug report by fiona cunningham:
    # change needed for PMC articles with only pmcId and no PMID
    #if len(elements)>1:
    if reqAttrName:
        filterElements = []
        for e in elements:
            if e.get(reqAttrName)==reqAttrValue:
                filterElements.append(e)
        elements = filterElements

    return elements

def toXmlString(element):
    return etree.tostring(element)

if __name__=="__main__":
    import doctest
    doctest.testmod()
