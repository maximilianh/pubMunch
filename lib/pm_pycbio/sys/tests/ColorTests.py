# Copyright 2006-2012 Mark Diekhans
import unittest, sys, cPickle
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.Color import Color
from pm_pycbio.sys.TestCaseBase import TestCaseBase


class ColorTests(TestCaseBase):
    def assertRgb(self, color, r, g, b):
        self.assertAlmostEquals(color.getRed(), r)
        self.assertAlmostEquals(color.getGreen(), g)
        self.assertAlmostEquals(color.getBlue(), b)

    def assertHsv(self, color, h, s, v):
        self.assertAlmostEquals(color.getHue(), h)
        self.assertAlmostEquals(color.getSaturation(), s)
        self.assertAlmostEquals(color.getValue(), v)

    def testRealRgb(self):
        c = Color.fromRgb(0.5, 0.3, 0.4)
        self.assertRgb(c, 0.5, 0.3, 0.4)
        self.assertRgb(c.setRed(0), 0.0, 0.3, 0.4)
        self.assertRgb(c.setGreen(1.0), 0.5, 1.0, 0.4)
        self.assertRgb(c.setBlue(0.2), 0.5, 0.3, 0.2)
        rgb = c.getRgb()
        self.assertAlmostEquals(rgb[0], 0.5)
        self.assertAlmostEquals(rgb[1], 0.3)
        self.assertAlmostEquals(rgb[2], 0.4)
        self.assertEquals(c.getRgb8(), (128, 77, 102))
        self.assertHsv(c, 0.9166666666, 0.4, 0.5)
        self.assertHsv(c.setHue(0.2), 0.2, 0.4, 0.5)
        self.assertHsv(c.setSaturation(0.2), 0.9166666666, 0.2, 0.5)
        self.assertHsv(c.setValue(1.0), 0.9166666666, 0.4, 1.0)
        hsv = c.getHsv()
        self.assertAlmostEquals(hsv[0], 0.9166666666)
        self.assertAlmostEquals(hsv[1], 0.4)
        self.assertAlmostEquals(hsv[2], 0.5)
        self.assertEquals(c.getHsvInt(), (330, 40, 50))
        self.assertEquals(c.toHtmlColor(), "#804d66")

    def testRegress(self):
        c = Color.fromRgb8(16, 78, 139)
        self.assertEquals(c.getRed8(), 16)
        self.assertEquals(c.getGreen8(), 78)
        self.assertEquals(c.getBlue8(), 139)
        self.assertEquals(c.toRgb8Str(), "16,78,139")
        self.assertEquals(c.toHsvIntStr(), "210,88,55")
        self.assertEquals(c.toHtmlColor(), "#104e8b")

# FIXME: many more tests needed

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ColorTests))
    return suite

if __name__ == '__main__':
    unittest.main()
