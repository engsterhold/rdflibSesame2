__author__ = 'robert'


from rdflib import URIRef, BNode, Literal
from rdflib.query import Result, ResultRow
try:
    from xml.etree import cElementTree as etree
except ImportError:
    from xml.etree import ElementTree as etree

#Constants
#The TriX namespace.
NAMESPACE = "{http://www.w3.org/2005/sparql-results#}"
HEAD = NAMESPACE+"head"
VARIABLE = NAMESPACE+"variable"
LINK = NAMESPACE+"link"
RESULTS = NAMESPACE+"results"
RESULT = NAMESPACE+"result"
URI = NAMESPACE+"uri"
LITERAL = NAMESPACE+"literal"
BNODE = NAMESPACE+"bnode"




class XMLResultParser(Result):


    triple = []

    def __init__(self, stream):
        """
        Constructs a new TrixParser for the given trix stream
        :param stream: A Trix stream
        :return:
        """

        self.runner = etree.iterparse(stream)

    #POSITION = 0



    def parse(self):
        """
        The actual parsing
        :return: yields statments + context
        """
        self.runner = iter(self.runner)
        _, root = next(self.runner)

        for event, element in self.runner:


            root.clear()


        #del self.runner





if __name__ == "__main__":
    pass

