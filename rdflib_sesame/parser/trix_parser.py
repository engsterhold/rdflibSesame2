__author__ = 'robert'


from rdflib import URIRef, BNode, Literal
from contextlib import closing
try:
    ### IMPORTANT: For some reason lxml does not work, with infered stuff in the respone, no idea why
    ### could be the trailing / vs #
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

    #context = None
    #next_uri_sets_context = False
    #triple = []

    def __init__(self, response):
        """
        Constructs a new TrixParser for the given trix stream
        :param response: A requests respone object, where streaming is enabled
        :return:
        """
        #self.runner = etree.iterparse(stream, events=EVENTS)
        self.response = response
    #POSITION = 0


    def __iter__(self):
        """
        Make object iterable
        :return:
        """
        yield from self._parse()



    def _parse(self):
        """
        The actual parsing
        :return: yields statments + context
        """
        with closing(self.response) as r:
            context = None
            runner = etree.iterparse(r.raw, events=EVENTS)
            runner = iter(runner)
            _, root = next(runner) # the root
            #print(root)
            for event, element in runner:
                if event == "start":
                    if element.tag == CONTEXT_TAG:
                        next_uri_sets_context = True
                    elif element.tag == TRIPLE_TAG:
                        next_uri_sets_context = False
                        triple = []
                elif event == "end":
                    if element.tag == URI_TAG:
                        if next_uri_sets_context:
                            context = URIRef(element.text)
                        else:
                            triple.append(URIRef(element.text))

                    elif element.tag == BNODE_TAG:
                            triple.append(BNode(element.text))

                    elif element.tag == PLAIN_LITERAL_TAG:
                        if LANGUAGE_ATT in element.attrib:
                            triple.append(Literal(element.text, lang=element.attrib[LANGUAGE_ATT]))
                        else:
                            triple.append(Literal(element.text))

                    elif element.tag == TYPED_LITERAL_TAG:
                        dt = URIRef(element.attrib['datatype'])
                        triple.append(Literal(element.text, datatype=dt))

                    elif element.tag == CONTEXT_TAG:
                        context = None
                        element.clear()
                    elif element.tag == TRIPLE_TAG and len(triple) == 3:
                        element.clear()
                        yield (triple[0],triple[1], triple[2]), context

                root.clear()



if __name__ == "__main__":
    pass
