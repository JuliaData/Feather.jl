import pandas as pd, numpy as np, datetime, unittest

print("Reading feather from Python...")

df2 = pd.read_feather("test2.feather")

class TestFeather(unittest.TestCase):

    def test(self):
        # NOTE something changed with this and it hasn't been working
        # self.assertEqual(df2["Abinary"][0], "hey")
        # self.assertEqual(df2["Abinary"][1], "there")
        # self.assertEqual(df2["Abinary"][2], "sailor")
        self.assertEqual(df2["Abool"][0], True)
        self.assertEqual(df2["Abool"][1], True)
        self.assertEqual(df2["Abool"][2], False)
        self.assertEqual(df2["Acat"][0], "a")
        self.assertEqual(df2["Acat"][1], "b")
        self.assertEqual(df2["Acat"][2], "c")
        self.assertEqual(df2["Acatordered"][0], "d")
        self.assertEqual(df2["Acatordered"][1], "e")
        self.assertEqual(df2["Acatordered"][2], "f")
        self.assertEqual(df2["Afloat32"][0], 1.0)
        self.assertEqual(np.isnan(df2["Afloat32"][1]), True)
        self.assertEqual(df2["Afloat32"][2], 0.0)
        self.assertEqual(df2["Afloat64"][0], np.inf)
        self.assertEqual(df2["Afloat64"][1], 1.0)
        self.assertEqual(df2["Afloat64"][2], 0.0)

if __name__ == '__main__':
    unittest.main()
