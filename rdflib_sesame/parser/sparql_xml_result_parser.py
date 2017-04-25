__author__ = 'robert'


from rdflib import URIRef, BNode, Literal, Variable
from contextlib import closing
from rdflib.query import Result, ResultRow
from collections import namedtuple
try:
    from xml.etree import cElementTree as etree
except ImportError:

    from xml.etree import ElementTree as etree

#Constants
#The SparqlResult namespace.
NAMESPACE = "{http://www.w3.org/2005/sparql-results#}"
HEAD = NAMESPACE+"head"
VARIABLE = NAMESPACE+"variable"
LINK = NAMESPACE+"link"
RESULTS = NAMESPACE+"results"
RESULT = NAMESPACE+"result"
URI = NAMESPACE+"uri"
LITERAL = NAMESPACE+"literal"
BNODE = NAMESPACE+"bnode"
BINDING = NAMESPACE+"binding"
BOOLEAN = NAMESPACE+"boolean"

#The attribute for language tags of plain literal.
LANGUAGE_ATT = '{http://www.w3.org/XML/1998/namespace}lang'

#The attribute for datatypes of typed literal.
DATATYPE_ATT = NAMESPACE+"datatype"

EVENTS = ("start", "end")


class XMLResultParser:


    def __init__(self, response):
        """
        Constructs a new TrixParser for the given trix stream
        :param stream: A Trix stream
        :return:
        """

        #self.response = response
        #print(self.response.raw)

        self.runner = etree.iterparse(response.raw, events=EVENTS)
        self.runner = iter(self.runner)
        self._extract_bindings()




    #POSITION = 0

    def __iter__(self):
            """
            Make object iterable
            :return:
            """
            yield from self._parse()



    def _extract_bindings(self):
        _, root = next(self.runner) # the root
        bindings = []
        for event, element in self.runner:
            #print(element.tag)
            if event == "end":
                if element.tag == VARIABLE:
                    bindings.append(Variable(element.attrib["name"]))
                elif element.tag==HEAD:
                    self.vars = bindings
                    element.clear()
                    break



    def _parse(self):

        _, root = next(self.runner) # the root
        if root.tag ==BOOLEAN:
            return True if root.text=="true" else False
        for event, element in self.runner:
            #print(element.tag)
            if event == "start":
                if element.tag == RESULT:
                    #ResultRow = namedtuple("ResultRow", self.vars)
                    t_res = {}
            elif event == "end":
                #t=None
                if element.tag == LITERAL:
                    if LANGUAGE_ATT in element.attrib:
                        t =Literal(element.text, lang=element.attrib[LANGUAGE_ATT])
                    elif 'datatype' in element.attrib:
                        dt = URIRef(element.attrib['datatype'])
                        t =Literal(element.text, datatype=dt)
                    else:
                        t =Literal(element.text)

                elif element.tag == URI:
                    t = URIRef(element.text)

                elif element.tag==BNODE:
                    t = BNode(element.text)

                elif element.tag == BINDING:
                        name = element.attrib['name']
                        t_res[Variable(name)]=t
                if element.tag == RESULT:
                    element.clear()
                    #print(t)
                    yield (t_res, self.vars)


            root.clear()

        #del self.runner





if __name__ == "__main__":
    pass

