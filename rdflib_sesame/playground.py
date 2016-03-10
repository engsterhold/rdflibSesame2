__author__ = 'robert'
from rdflib.resource import Resource, URIRef, BNode
from rdflib.namespace import *
from rdflib import Graph, ConjunctiveGraph, Dataset, Literal
from rdflib.store import Store
from rdflib.plugin import get as plugin
import rdflib.extras.infixowl as ont
from rdflib.store import Store, NO_STORE, VALID_STORE
import time
import requests
from collections import Iterable
import json
from rdflib_sesame.utils import QuadStreamParser
from rdflib_sesame.binary_rdf_parser import BinaryRDFParser


RES = URIRef("http://127.0.0.1:6543/atlas/mrhsa/resource/74e1d7f2-f3d9-43de-9d89-f26d6e09a266")
PRED = URIRef("http://127.0.0.1:6543/properties/map#werkbeschreibung")

LIT = Literal("Strodthagen")
LIT2 = Literal("bose")

CTX1 =URIRef("http://127.0.0.1:6543/geodata")
CTX2 = URIRef("http://127.0.0.1:6543/atlas/mrhsa")


class SesameStore(Store):
    context_aware = True
    graph_aware = True

    def __init__(self, base_url, repository):
        self.qsp = QuadStreamParser()
        #self.sess = requests.Session()
        self.base_url = base_url
        self.repositories = self.base_url+"/repositories"
        self.protocol = self.base_url+"/protocol"
        self.repository = self.repositories+"/"+repository

    def triples(self, triple_pattern, context=None, infer=True, accept="text/x-nquads"):
        """
        FIXME: Improve perfomance on the parsing oder even better get somehow the stream request to work.
        TODO: allow one or more contexts
        :param triple_pattern: The triple pattern.
        :param context: The contexts
        :param infer: include inferred triples, default = false
        :return: yields (s p o), context
        """
        s,p,o = triple_pattern
        uri = self.repository+"/statements"
        payload = dict(infer=infer)
        if s:
            payload["subj"] = s.n3()
        if p:
            payload["pred"] = p.n3()
        if o:
            payload["obj"] = o.n3()
        if context:
            payload["context"] = [context.identifier.n3()]
        payload["infer"] = infer
        r = requests.get(uri, params=payload,stream=True, headers = {"Accept" : accept,
                                                                      'connection': 'keep-alive',
                                                                     'transfer-encoding': 'chunked',
                                                                     'Accept-Encoding': 'gzip,deflate'})


        if r.headers["content-type"] == "application/rdf+xml":
            pass
        elif r.headers["content-type"] == "text/plain":
            pass
        elif r.headers["content-type"] == "text/turtle":
            pass
        elif r.headers["content-type"] == "text/rdf+n3":
            pass
        elif r.headers["content-type"] == "text/x-nquads":
            pass
        elif r.headers["content-type"] == "application/rdf+json":
            pass
        elif r.headers["content-type"] == "application/trix":
            pass
        elif r.headers["content-type"] == "application/x-trig":
            pass
        elif r.headers["content-type"] == "application/x-binary-rdf;charset=UTF-8":
            #print("iam here")
            #with open("bintest.brf", "wb") as b:
            #    b.write(r.content)
            u = BinaryRDFParser(r.content)
            print (list(u.parse()))




    def query(self, query, initNs, initBindings, queryGraph, **kwargs):
        uri = self.repository








    def contexts(self, triple=None):
        uri = self.repository+"/contexts"
        if not triple:
            r = requests.get(uri, headers = {"Accept" : "application/sparql-results+json"})
            return r.text
        else:
            raise "Not yet implemented"


    def __len__(self, context=None):
        uri = self.repository+"/size"
        payload=dict()
        #if context:
        #    payload["context"] = context.identifier.n3()
            #r = requests.get(uri, params = payload)

        r = requests.get(uri, params=payload)
        return int(r.text)



if __name__ == "__main__":
    #f = Dummy()
    sto = SesameStore("http://localhost:18080/graphdb%2Dworkbench%2Dfree/","rede")

    sto.triples((RES, URIRef("http://127.0.0.1:6543/properties/map#mapid"), None), accept="application/x-binary-rdf")
       # print (i)
