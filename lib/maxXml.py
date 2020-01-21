#!/usr/bin/env python
import logging, urllib
import xml.etree.cElementTree as etree

class XmlParser(object):
    """ class to represent an xml tree (using ElementTree)
        Functions Accept PATH which is a /a/b/c style xpath-like expression to refer to elements
        PATH is not a complete XPATH implementation

        getText... functions return just a string
        getXml... functions return an XmlParser-object
        ...First  functions get only the first instance
        ...All    functions return an iterator

    >>> xp = XmlParser(string="<fruit><apple size='big'>boskoop</apple><apple size='small'>granny smith</apple><pear>mypear</pear></fruit>")
    >>> xp.getTextFirst("pineapple", default="NothingAtAll")
    'NothingAtAll'
    >>> xp.getTextFirst("apple")
    'boskoop'
    >>> list(xp.getTextAll("apple"))
    ['boskoop', 'granny smith']
    >>> list(xp.getTextAll("apple", reqAttrDict={'size':'big'}))
    ['boskoop']

    """
    def __init__(self, string=None, url=None, root=None, removeNamespaces=False):
        self.root=None
        if string!=None:
            self.fromString(string, removeNamespaces)
        elif url!=None:
            self.fromUrl(url, removeNamespaces)
        elif root!=None:
            self.root=root

    def getAttr(self, name):
        return self.root.attrib.get(name, None)

    def getText(self):
        if self.root.text==None:
            return ""
        else:
            return self.root.text

    def getTextTail(self):
        if self.root.tail==None:
            return ""
        else:
            return self.root.tail

    def fromString(self, string, removeNamespaces=False):
        if string=="":
            return None
        root = etree.fromstring(string)
        if removeNamespaces:
            logging.debug("Stripping all namespaces")
            strip_namespace_inplace(root)
        self.root = root

    def fromUrl(self, url, removeNamespaces=False, stopWords=[]):
        logging.debug("Retrieving %s" % url)
        text = urllib.urlopen(url).read()
        self.fromString(text, removeNamespaces=removeNamespaces)
        #for w in stopWords:
            #if w in text:
                #return None

    def _removeNamespaces(self):
        """ removes all namespaces from elementtree IN PLACE """
        root = self.root
        for el in root.getiterator():
            if el.tag[0] == '{':
                el.tag = el.tag.split('}', 1)[1]

    def _hasAttribs(self, el, reqAttrDict):
        for attr, value in reqAttrDict.iteritems():
            if el.attrib.get(attr, None)!=value:
                return False
        return True

    def getTextFirst(self, path, reqAttrDict=None, default=None):
        """ return text between elements given path
            reqAttrDict is in the format attrName -> value
        """
        xml = self.getElFirst(path, reqAttrDict)
        if xml != None and xml.text!=None:
            return xml.text
        else:
            return default

    def getTextAll(self, path, reqAttrDict=None):
        for el in self.getElAll(path, reqAttrDict):
            yield el.text

    def getElFirst(self, path, reqAttrDict):
        found = False
        for el in self.getElAll(path, reqAttrDict):
            found = True
            return el

    def getElAll(self, path, reqAttrDict):
        found = False
        elIter = self.root.findall(path)
        for el in elIter:
            if reqAttrDict == None or self._hasAttribs(el, reqAttrDict):
                found = True
                yield el

    def getXmlFirst(self, path, reqAttrDict=None, default=None):
        el = self.getElFirst(path, reqAttrDict)
        if el==None:
            return default
        else:
            return XmlParser(root=el)

    def getXmlAll(self, path, reqAttrDict=None):
        for el in self.getElAll(path, reqAttrDict):
            yield XmlParser(root=el)

    def __repr__(self):
        return etree.tostring(self.root)

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

# -----
if __name__ == "__main__":
    import doctest
    doctest.testmod()
