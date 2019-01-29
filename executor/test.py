import doctest
import unittest

from executor import executor

def test_suite():
    suite = unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(executor))

    return suite

if __name__ == '__main__':
    doctest.testmod(executor)
