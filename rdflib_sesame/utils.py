__author__ = 'robert'

from rdflib.plugins.parsers.ntriples import NTriplesParser
from rdflib.plugins.parsers.ntriples import ParseError
from rdflib.plugins.parsers.ntriples import r_tail
from rdflib.plugins.parsers.ntriples import r_wspace
from rdflib.plugins.parsers.ntriples import r_wspaces





class QuadStreamParser(NTriplesParser):

    def __init__(self):
        pass

    def consume(self, line):
        self.line = line
        self.eat(r_wspace)
        if (not self.line) or self.line.startswith(('#')):
            return  # The line is empty or a comment

        subject = self.subject()
        self.eat(r_wspace)

        predicate = self.predicate()
        self.eat(r_wspace)

        obj = self.object()
        self.eat(r_wspace)

        context = self.uriref() or self.nodeid()
        self.eat(r_tail)

        if self.line:
            raise ParseError("Trailing garbage")
        # Must have a context aware store - add on a normal Graph
        # discards anything where the ctx != graph.identifier
        #self.sink.get_context(context).add((subject, predicate, obj))
        return (subject, predicate, obj)







if __name__ == "__main__":
    pass
