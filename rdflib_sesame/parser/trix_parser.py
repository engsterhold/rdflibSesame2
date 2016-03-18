__author__ = 'robert'


from rdflib import URIRef, BNode, Literal

try:
    from xml.etree import cElementTree as etree
except ImportError:
    from xml.etree import ElementTree as etree

#Constants
#The TriX namespace.
NAMESPACE = "{http://www.w3.org/2004/03/trix/trix-1/}"

#The root tag.
ROOT_TAG = NAMESPACE+"TriX"

#The tag that starts a new context/graph.
CONTEXT_TAG = NAMESPACE+"graph"

#The tag that starts a new triple.
TRIPLE_TAG = NAMESPACE+"triple"

#The tag for URI values.
URI_TAG = NAMESPACE+"uri"

#The tag for BNode values.
BNODE_TAG = NAMESPACE+"id"

#The tag for plain literal values.
PLAIN_LITERAL_TAG = NAMESPACE+"plainLiteral"

#The tag for typed literal values.
TYPED_LITERAL_TAG = NAMESPACE+"typedLiteral"

#The attribute for language tags of plain literal.
LANGUAGE_ATT = '{http://www.w3.org/XML/1998/namespace}lang'

#The attribute for datatypes of typed literal.
DATATYPE_ATT = NAMESPACE+"datatype"



EVENTS = ("start", "end")


class TrixParser:

    context = None
    next_uri_sets_context = False
    triple = []

    def __init__(self, stream):
        """
        Constructs a new TrixParser for the given trix stream
        :param stream: A Trix stream
        :return:
        """

        self.runner = etree.iterparse(stream, events=EVENTS)

    #POSITION = 0



    def parse(self):
        """
        The actual parsing
        :return: yields statments + context
        """
        self.runner = iter(self.runner)
        _, root = next(self.runner)

        for event, element in self.runner:
            if event == "start":
                if element.tag == CONTEXT_TAG:
                    self.next_uri_sets_context = True
                elif element.tag == TRIPLE_TAG:
                    self.next_uri_sets_context = False
                    self.triple = []
            elif event == "end":
                if element.tag == URI_TAG:
                    if self.next_uri_sets_context:
                        self.context = URIRef(element.text)
                    else:
                        self.triple.append(URIRef(element.text))

                elif element.tag == BNODE_TAG:
                    self.triple.append(BNode(element.text))

                elif element.tag == PLAIN_LITERAL_TAG:
                    if LANGUAGE_ATT in element.attrib:
                        self.triple.append(Literal(element.text, lang=element.attrib[LANGUAGE_ATT]))
                    else:
                        self.triple.append(Literal(element.text))

                elif element.tag == TYPED_LITERAL_TAG:
                    dt = URIRef(element.attrib['datatype'])
                    self.triple.append(Literal(element.text, datatype=dt))

                elif element.tag == CONTEXT_TAG:
                    self.context = None
                    element.clear()
                elif element.tag == TRIPLE_TAG and len(self.triple) == 3:
                    element.clear()
                    yield  (self.triple[0],self.triple[1], self.triple[2]), self.context
            root.clear()


        #del self.runner

    def clear_element(self,element):
        element.clear()
        while element.getprevious() is not None:
            del element.getparent()[0]



if __name__ == "__main__":
    pass
