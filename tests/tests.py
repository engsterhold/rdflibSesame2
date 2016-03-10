__author__ = 'robert'
import random
import unittest
from rdflib_sesame import SesameStore

class TestStore(unittest.TestCase):

    def setUp(self):
        self.store = SesameStore("http://localhost:18080/graphdb%2Dworkbench%2Dfree/","rede")



    def test_len(self):
        print(len(self.store))

    def tearDown(self):
        del self.store

if __name__ == '__main__':
    unittest.main()