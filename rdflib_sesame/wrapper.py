__author__ = 'robert'
#from rdflib.plugins.stores.sparqlstore import SPARQLStore
import logging
from contextlib import closing
from io import BytesIO, StringIO
import re
from rdflib.plugin import get as plugin
from rdflib import Graph, URIRef, BNode, Literal, Variable, ConjunctiveGraph, Dataset
from rdflib.store import Store
import requests
from rdflib.query import Result
import time
from rdflib_sesame.parser import BinaryRDFParser, TrixParser, XMLResultParser
from rdflib_sesame.serializers import TrigSerializer2

pattern = re.compile(r"""
    ((?P<base>(\s*BASE\s*<.*?>)\s*)|(?P<prefixes>(\s*PREFIX\s+.+:\s*<.*?>)\s*))*
    (?P<queryType>(CONSTRUCT|SELECT|ASK|DESCRIBE|INSERT|DELETE|CREATE|CLEAR|DROP|LOAD|COPY|MOVE|ADD))
""", re.VERBOSE | re.IGNORECASE)


NULL_CONTEXT = URIRef("http://www.openrdf.org/schema/sesame#nil")

class Dummy:
    def __init__(self):
        print("dummy")


class SesameStore(Store):


    def __init__(self, base_url, repository,**kwargs):
        self.context_aware = True
        self.graph_aware = True


        self.__init_services(base_url, repository)
        #self.identifier = self.rest_services["repository"]
        #self.sess = requests.Session()
        #self.base_url = base_url
        #self.repositories = self.base_url+"/repositories"
        #self.protocol = self.base_url+"/protocol"
        #self.repository = self.repositories+"/"+repository
        self.infer = kwargs.pop("infer", True)
        self.trx_batch = kwargs.pop("trx_batch", 100000)
        self.commit_batch = kwargs.pop("commit_batch", 10000)
        self.commit_batch = min(self.trx_batch,self.commit_batch)
        super(SesameStore, self).__init__(identifier = self.rest_services["repository"])


    def __init_services(self, base_url, repository):
        """
        initilase the rest_service map with the neccessary urls
        :param base_url:
        :param repository:
        :return:
        """
        self.rest_services = {}
        self.rest_services["protocol"] = base_url+"/protocol"
        self.rest_services["repositories"] = base_url+"/repositories"
        self.rest_services["repository"] = base_url+"/repositories/{}".format(repository)
        self.rest_services["statements"] = self.rest_services["repository"]+"/statements"
        self.rest_services["contexts"] = self.rest_services["repository"]+"/contexts"
        self.rest_services["size"] = self.rest_services["repository"]+"/size"
        self.rest_services["transaction"] = self.rest_services["repository"]+"/transactions"


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
        if context and isinstance(context.identifier, URIRef):
            payload["context"] = [context.identifier.n3()] #FIXME
        #payload["context"] = set(["<http://127.0.0.1:6543/atlas/wa>","<http://127.0.0.1:6543/atlas/dwaln>", "<http://127.0.0.1:6543/atlas/mrhsa>", "<http://127.0.0.1:6543/geodata>" ])
        #payload["context"] = set(["<http://127.0.0.1:6543/atlas/mrhsa>"])

        payload["infer"] = self.infer if infer is None else infer
        payload["infer"] = str(payload["infer"]).lower()
        #with closing(requests.get(uri, params=payload,stream=True, headers = {"Accept" : "application/x-binary-rdf",
        #                                                              'connection': 'keep-alive',
        #                                                             'transfer-encoding': 'chunked',
        #                                                            'Accept-Encoding': 'gzip,deflate'})) as r:
        #    #r.raw.decode_content = True
        #    for i in BinaryRDFParser(r.content).parse():
        #       yield i
        r =requests.get(uri, params=payload, stream=True, headers = {"Accept" : "application/trix",
                                                                       'connection': 'keep-alive',
                                                                      'transfer-encoding': 'chunked',
                                                                      'Accept-Encoding': 'gzip,deflate'})

        r.raw.decode_content = True
        return self.__make_trix_generator__(r)



    def _new_transaction(self):
        try:
            uri = self.rest_services["transaction"]
            r = requests.post(uri)
            trx = r.headers["location"]
            return trx
        except:
            logging.warning("entered trx exception")
            for i in range(5):
                logging.warning("sleep trx", i)
                time.sleep(i)
                try:
                    uri = self.rest_services["transaction"]
                    r = requests.post(uri)
                    trx = r.headers["location"]
                    return trx
                    break
                except:
                    logging.error("still trx error: ", data)


    def _rollback(self, trx):
        r = requests.delete(trx)
        raise Exception("transaction aborted, rolled back")

    def _commit(self,trx):
        r = requests.put(trx, params={"action" : "COMMIT"})
        #print("commit",r)

    def _add(self, trx, data, payload):

        r = requests.put(trx, data=data, params=payload,
                          headers={"Content-Type" :"text/plain;charset=UTF-8"} )
        #print(r)
        if r.status_code ==200:
            return r.status_code
        else:
            logging.error("Status not 200/ok")
            raise r.raise_for_status()

    def _repair_context(self, context):
        """
        Because of some wonky mechanics in rdflib, context can either be a graph,
        a URIRef oder a BNode. we are only interested in the URIRef as a context
        IMPORTANT: Only accepts URIRefs as valid context identifactors. everything else get set to NULL_CONTEXT
        :context  the context to check
        """

        if isinstance(context, Graph):
            return self._repair_context(context.identifier)
        elif isinstance(context, URIRef):
            return context
        else:
            return NULL_CONTEXT


    def _addN(self, trx, data, payload):

        r = requests.put(trx, data=data, params=payload,
                          headers={"Content-Type" :"text/x-nquads;charset=UTF-8"} )
        #print(r)
        if r.status_code ==200:
            return r.status_code
        else:
            raise r.raise_for_status()

    def add_graph(self, graph):
        graph = self._repair_context(graph)
        return Graph(self, identifier=graph)


    def addN(self, quads):

        uri = self.rest_services["statements"]
        payload = {"action":"ADD"}
        #print("quads", quads)

        trx = self._new_transaction()
        try:
            cache = ConjunctiveGraph()
            for n, (s,p,o,c) in enumerate(quads):

                c = self._repair_context(c)
                #print("loop",n, (s,p,o,c) )
                #x ="{} {} {} {} .".format(s.n3(), p.n3(), o.n3(), c.n3() if c else "")
                cache.add((s,p,o,c))
                if n%self.commit_batch==self.commit_batch-1:
                    data = cache.serialize(format="nquads")
                    #data = data.encode("utf8")
                    self._addN(trx, data, payload)
                    data =None
                    cache = ConjunctiveGraph()
                    logging.debug("Cache flusehd")
                if n%self.trx_batch==self.trx_batch-1:
                    logging.debug("{} triples commited".format(n%self.trx_batch))
                    self._commit(trx)
                    trx = self._new_transaction()

            data = cache.serialize(format="nquads")
            #print(data)
            self._addN(trx, data, payload)
            self._commit(trx)
            data = None
        except Exception as why:
            logging.exception(why)
            self._rollback(trx)



    def add(self, spo, context=None, quoted=False):
        """
        Single statement add.
        Not really happy right now. Will make problems with triple quotes
        :param spo: The triple to add
        :param context: The context
        :param quoted: Dont know yet
        :return: The status code (FIXME)
        """

        #s,p,o = spo
        context =self._repair_context(context)
        uri = self.rest_services["statements"]
        payload = {"action":"ADD"}
        if context != NULL_CONTEXT:
            payload["context"] = context.n3()
        g = Graph(identifier=context)
        g.add(spo)
        #s, p, o = spo
        #nt = "{} {} {} .".format(s.n3(), p.n3(), o.n3())
        data = g.serialize(format="ntriples")
        #data = data.encode("utf8")
        trx = self._new_transaction()
        try:
            self._add(trx, data, payload)
            self._commit(trx)
        except:
            logging.error("add error:", data)
            self._rollback(trx)
            #raise Exception("add rolled back")
            n=0
            for i in range(5):
                logging.warning("add sleep", i)
                time.sleep(i)
                trx = self._new_transaction()
                try:
                    self._add(trx, data, payload)
                    self._commit(trx)
                    break
                except:
                    logging.warning("still error: ", data, i)
                    self._rollback(trx)
                    n =n+1
            if n == 4:
                raise Exception("add rolled back after 5 retries")


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
        """
        check for triples not yet supportet
        :param triple: optional triple
        :return: the context as a Result
        """
        uri = self.rest_services["contexts"]
        if not triple:
            r = requests.get(uri, stream=True,headers = {"Accept" : "application/sparql-results+json",
                                             'connection': 'keep-alive',
                                             'Accept-Encoding': 'gzip,deflate',
                                             })
            r.raw.decode_content = True
            return Result.parse(r.raw, "json")
        else:
            raise NotImplementedError


    def __len__(self, context=None):
        """
        Does not support an aggregation of contexts (yet)
        :param context: the context to check size of, all if None
        :return: the size as an int
        """
        if context is not None:
            context = self._repair_context(context)
        uri = self.rest_services["size"]
        payload=dict()
        if context:
            context = context.n3()
            payload["context"] = context
        r = requests.get(uri, params = payload)
        return int(r.text)



if __name__ == "__main__":
    Sesame = plugin("Sesame", Store)
    store = Sesame("http://localhost:7200", "playground")
    ctx = URIRef("urn:ctx2/add_trig_test")
    #ds = Dataset(store)
    #g = Graph(store=store, identifier=ctx)
    g = ConjunctiveGraph(store)
    #g = ds.graph()
    bt = []
    for i in range(3):
        a = URIRef("urn:add2#Subj_add{}".format(i))
        b = URIRef("urn:add3#Pred_add{}".format(i))
        c = Literal("erfolg_ng_add{}".format(i), lang="de")
        g.add((a,b,c))
    for i in range(3):
        a = URIRef("urn:add2#Subj_addn{}".format(i))
        b = URIRef("urn:add3#Pred_addn{}".format(i))
        c = Literal("erfolg_ng_addn{}".format(i), datatype="urn:dummy")
        bt.append((a,b,c,g))


    g.addN(bt)
    print(len(g))
    print(list(g.triples((None, None, None))))


    #for i in ds.triples((None, None, None)):
        #print(i)
