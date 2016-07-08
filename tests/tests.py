__author__ = 'robert'
import random
import unittest
from rdflib_sesame import SesameStore
from rdflib import Graph

CT = "Construct {?s a ?o} where {?s a ?o ; rdfs:label 'Bach'  }"

QR = "Select ?s ?o where { ?s a ?o ; rdfs:label 'Bach' }"

AK = "Ask where { ?s a ?o . ?s rdfs:label 'Bach'}"




class TestStore(unittest.TestCase):

    def setUp(self):
        self.store = SesameStore("http://localhost:7200","rede")
        self.graph = Graph(self.store)



    def test_len(self):
        print(len(self.store))



    def test_construct_query(self):
        s = set()
        for i, _ in self.graph.query(CT):
            s.add(i[0])
        self.assertEqual(len(s), 38)



    def test_select_query(self):
        qr = self.graph.query(QR)

        self.assertEqual([str(c) for c in qr.vars], ['s', 'o'])



    def test_ask_query(self):

        self.assertTrue(self.graph.query(AK))




    def tearDown(self):
        del self.store

if __name__ == '__main__':
    unittest.main()