
__author__ = 'robert'
#from rdflib.plugins.stores.sparqlstore import SPARQLStore
from contextlib import closing
from io import BytesIO, StringIO
import re
from rdflib.plugin import get as plugin
from rdflib import Graph, URIRef, BNode, Literal, Variable, ConjunctiveGraph, Dataset
from rdflib.store import Store
from rdflib.query import Result
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from urllib.parse import urlencode, quote_plus
from rdflib_sesame.parser import BinaryRDFParser, TrixParser, XMLResultParser

pattern = re.compile(r"""
    ((?P<base>(\s*BASE\s*<.*?>)\s*)|(?P<prefixes>(\s*PREFIX\s+.+:\s*<.*?>)\s*))*
    (?P<queryType>(CONSTRUCT|SELECT|ASK|DESCRIBE|INSERT|DELETE|CREATE|CLEAR|DROP|LOAD|COPY|MOVE|ADD))
""", re.VERBOSE | re.IGNORECASE)

class Dummy:
    def __init__(self):
        print("dummy")


class SesameStore(Store):


    rest_services = {}

    def __init__(self, base_url, repository, infer=True):

        self.__init_services(base_url, repository)
        self.context_aware = True
        self.graph_aware = True
        #self.identifier = self.rest_services["repository"]
        #self.sess = requests.Session()
        #self.base_url = base_url
        #self.repositories = self.base_url+"/repositories"
        #self.protocol = self.base_url+"/protocol"
        #self.repository = self.repositories+"/"+repository
        self.infer = infer
        self.http_client = AsyncHTTPClient()
        super(SesameStore, self).__init__(identifier = self.rest_services["repository"])


    def __init_services(self, base_url, repository):
        """
        initilase the rest_service map with the neccessary urls
        :param base_url:
        :param repository:
        :return:
        """

        self.rest_services["protocol"] = base_url+"/protocol"
        self.rest_services["repositories"] = base_url+"/repositories"
        self.rest_services["repository"] = base_url+"/repositories/{}".format(repository)
        self.rest_services["statements"] = self.rest_services["repository"]+"/statements"
        self.rest_services["contexts"] = self.rest_services["repository"]+"/contexts"
        self.rest_services["size"] = self.rest_services["repository"]+"/size"
        self.rest_services["transaction"] = self.rest_services["repository"]+"/transactions"


    @gen.coroutine
    def triples(self, triple_pattern, context=None, infer=None):
        """
        FIXME: Improve perfomance on the parsing oder even better get somehow the stream request to work.
        TODO: allow one or more contexts
        :param triple_pattern: The triple pattern.
        :param context: The contexts
        :param infer: include inferred triples, default = false
        :return: yields (s p o), context
        """
        s,p,o = triple_pattern
        uri = self.rest_services["statements"]
        payload = dict(infer=infer)
        if s:
            if not isinstance(s, (URIRef, BNode)):
                return self.__yield_empty()
            else:
                payload["subj"] = s.n3()
        if p:
            if not isinstance(p, (URIRef, BNode)):
                self.__yield_empty()
            else:
                payload["pred"] = p.n3()
        if o:
            if not isinstance(o, (URIRef, BNode, Literal)):
                self.__yield_empty()
            else:
                payload["obj"] = o.n3()
        #if context and isinstance(context.identifier, URIRef):
        #    payload["context"] = [context.identifier.n3()] #FIXME
        #payload["context"] = set(["<http://127.0.0.1:6543/atlas/wa>","<http://127.0.0.1:6543/atlas/dwaln>", "<http://127.0.0.1:6543/atlas/mrhsa>", "<http://127.0.0.1:6543/geodata>" ])
        #payload["context"] = set(["<http://127.0.0.1:6543/atlas/mrhsa>"])

        payload["infer"] = self.infer if infer is None else infer
        payload["infer"] = str(payload["infer"]).lower()

        qs = urlencode(payload, quote_via=quote_plus)
        uri= "{}?{}".format(uri, qs)
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(uri, method="GET",headers= {"Accept" : "application/trix",
                                                                       'connection': 'keep-alive',
                                                                      'transfer-encoding': 'chunked',
                                                                      'Accept-Encoding': 'gzip,deflate'})


        return self.__make_trix_generator__(response)



    def _new_transaction(self):
        uri = self.rest_services["transaction"]
        r = yield self.http_client.fetch(uri, method="POST")
        trx = r.headers["location"]
        #print(trx)
        return trx

    def _rollback(self, trx):
        r = yield self.http_client.fetch(trx, method="DELETE")
        print("rollback",r)

    def _commit(self,trx):
        payload = {"action" : "COMMIT"}
        qs = urlencode(payload, quote_via=quote_plus)
        uri= "{}?{}".format(trx, qs)
        r = yield self.http_client.fetch(uri, method="PUT")
        #print("commit",r)

    def _add(self, trx, data, payload):

        qs = urlencode(payload, quote_via=quote_plus)
        uri= "{}?{}".format(trx, qs)
        r = yield self.http_client.fetch(uri, method="POST",body=data,
                          headers={"Content-Type" :"text/turtle;charset=UTF-8"} )
        return r.status_code

    def add(self, spo, context=None, quoted=False):
        """
        Single statement add.
        TODO: Transactions
        :param spo: The (triple)
        :param context:
        :param quoted: Dont know yet
        :return: The status code (FIXME)
        """

        #s,p,o = spo
        uri = self.rest_services["statements"]
        payload = {"action":"ADD"}
        if context and not isinstance(context.identifier, BNode):
            payload["context"] = context.identifier.n3()
        g = Graph(identifier=context if context else None)
        g.add(spo)

        data = g.serialize(format="turtle")
        #data = data.decode("utf-8")
        #print(data)


        trx = self._new_transaction()

        if self._add(trx, data, payload) != 200:
            self._rollback(trx)
        else:
            self._commit(trx)


    def remove(self, spo, context=None):
        """
        Removes a statement from the triple store
        :param spo: the statenent
        :param context: the context to remove from
        :return:
        """

        uri = self.rest_services["statements"]
        s,p,o = spo
        payload = dict()
        if s:
            payload["subj"] = s.n3()
        if p:
            payload["pred"] = p.n3()
        if o:
            payload["obj"] = o.n3()
        if context:
            payload["context"] = [context.n3()]

        #data = " ".join(i.n3() for i in spo) +" ."
        #print(data)
        r = requests.delete(uri, params=payload)



    def query(self, query, initNs=None, initBindings=None, queryGraph=None, **kwargs):
        """
        Performs a sparql query angainst the endpoint
        TODO: Some query refinement and error checking
        TODO: Streaming response.
        :param query: The query
        :param initNs: Initial namespaces, unused afaik
        :param initBindings: Initial bindings
        :param queryGraph: The Graph
        :param kwargs: {infer: True/False}
        :return: A rdflib.Result
        """

#        r_queryType = pattern.search(query).group("prefixes").upper()
#        print(r_queryType)
        uri = self.rest_services["repository"]
        infer = kwargs.get('infer',None)
        #timeout = kwargs.get('timeout',"0")
        payload = {"$"+k: v.n3() for k,v in initBindings.items()}

        payload["infer"] = self.infer if infer is None else infer
        payload["infer"] = str(payload["infer"]).lower()
        #payload["$"+timeout]=0
        payload["query"] = query
        r = requests.post(uri, data=payload,
                                   stream=True,
                                   headers= {"Accept" : "application/sparql-results+json,application/trix",
                                             'connection': 'keep-alive',
                                             'Accept-Encoding': 'gzip,deflate',
                                             "Content-Type" :"application/x-www-form-urlencoded"})

        r.raw.decode_content = True
        if r.headers['Content-Type'] == 'application/sparql-results+json;charset=UTF-8':
            return self.__make_result(r)
        elif r.headers['Content-Type'] == 'application/trix;charset=UTF-8':
            return self.__make_trix_generator__(r)
        else:
            raise ValueError("Response content type not parsable {r}".format(r=r.text))


    def __make_trix_generator__(self, response):
        """
        result heisst jetzt trix sonst ändert sich nichts.
        Helper method to encapsulate the streaming trix parsing
        :param a raw response object
        """
        return TrixParser(response)


    def __yield_empty(self):

        if False:
            yield


    def __make_result(self, response):
        #return XMLResultParser(response)
        return Result.parse(response.raw, format="json")

    def contexts(self, triple=None):
        uri = self.rest_services["contexts"]
        if not triple:
            r = requests.get(uri, headers = {"Accept" : "application/sparql-results+json"})
            return Result.parse(r.raw, "json")
        else:
            raise "Not yet implemented"


    def __len__(self, context=None):
        uri = self.rest_services["size"]
        payload=dict()
        #print(context.n3())
        #if context:
        #    payload["context"] = context.identifier.n3()
            #r = requests.get(uri, params = payload)

        r = yield self.http_client.fetch(uri, method="GET")
        return int(r.text)



if __name__ == "__main__":
    #Sesame = SesameStore()
    store = SesameStore("http://localhost:7200", "playground")
    ctx = URIRef("urn:ctx/add_ng")
    ds = Graph(store)
    #graph = Graph(identifier=ctx)
    #g = ds.graph(ctx)
    a = URIRef("urn:add#Subj")
    b = URIRef("urn:add#Pred")
    c = Literal("erfolg_ng")

    #graph.add((a,b,c))

    for i in ds.triples((None, b, None)):
        print(i)
