import sys
import unittest

from html5lib.filters.formfiller import SimpleFilter

class FieldStorage(dict):
    def getlist(self, name):
        l = self[name]
        if isinstance(l, list):
            return l
        elif isinstance(l, tuple) or hasattr(l, '__iter__'):
            return list(l)
        return [l]

class TestCase(unittest.TestCase):
    def runTest(self, input, formdata, expected):
        try:
            output = list(SimpleFilter(input, formdata))
        except NotImplementedError as nie:
            # Amnesty for those that confess...
            print("Not implemented:", str(nie), file=sys.stderr)
        else:
            errorMsg = "\n".join(["\n\nInput:", str(input),
                                  "\nForm data:", str(formdata),
                                  "\nExpected:", str(expected),
                                  "\nReceived:", str(output)])
            self.assertEquals(output, expected, errorMsg)

    def testSingleTextInputWithValue(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "text"), ("name", "foo"), ("value", "quux")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "text"), ("name", "foo"), ("value", "bar")]}])

    def testSingleTextInputWithoutValue(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "text"), ("name", "foo")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "text"), ("name", "foo"), ("value", "bar")]}])

    def testSingleCheckbox(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "bar")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "bar"), ("checked", "")]}])

    def testSingleCheckboxShouldBeUnchecked(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "quux")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "quux")]}])

    def testSingleCheckboxCheckedByDefault(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "bar"), ("checked", "")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "bar"), ("checked", "")]}])

    def testSingleCheckboxCheckedByDefaultShouldBeUnchecked(self):
        self.runTest(
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "quux"), ("checked", "")]}],
            FieldStorage({"foo": "bar"}),
            [{"type": "EmptyTag", "name": "input",
                "data": [("type", "checkbox"), ("name", "foo"), ("value", "quux")]}])

    def testSingleTextareaWithValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "textarea", "data": [("name", "foo")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "textarea", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "textarea", "data": [("name", "foo")]},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "textarea", "data": []}])

    def testSingleTextareaWithoutValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "textarea", "data": [("name", "foo")]},
             {"type": "EndTag", "name": "textarea", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "textarea", "data": [("name", "foo")]},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "textarea", "data": []}])

    def testSingleSelectWithValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectWithValueShouldBeUnselected(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "quux"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectWithoutValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("selected", "")]},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectWithoutValueShouldBeUnselected(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "quux"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectTwoOptionsWithValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectTwoOptionsWithValueShouldBeUnselected(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "baz")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "quux"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "baz")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectTwoOptionsWithoutValue(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "bar"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("selected", "")]},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectTwoOptionsWithoutValueShouldBeUnselected(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "baz"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": "quux"}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "bar"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": []},
             {"type": "Characters", "data": "baz"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testSingleSelectMultiple(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo"), ("multiple", "")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": ["bar", "quux"]}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo"), ("multiple", "")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

    def testTwoSelect(self):
        self.runTest(
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []},
             {"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}],
            FieldStorage({"foo": ["bar", "quux"]}),
            [{"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []},
             {"type": "StartTag", "name": "select", "data": [("name", "foo")]},
             {"type": "StartTag", "name": "option", "data": [("value", "bar")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "StartTag", "name": "option", "data": [("value", "quux"), ("selected", "")]},
             {"type": "Characters", "data": "quux"},
             {"type": "EndTag", "name": "option", "data": []},
             {"type": "EndTag", "name": "select", "data": []}])

def buildTestSuite():
    return unittest.defaultTestLoader.loadTestsFromName(__name__)

def main():
    buildTestSuite()
    unittest.main()

if __name__ == "__main__":
    main()
