__author__ = 'robert'


from rdflib.plugins.stores.sparqlstore import SPARQLStore
from rdflib.store import Store, NO_STORE, VALID_STORE
from rdflib.resource import Resource, URIRef, BNode
from rdflib.namespace import *
from rdflib import Graph, ConjunctiveGraph, Dataset, Literal
from rdflib.store import Store
from rdflib.plugin import get as plugin
import rdflib.extras.infixowl as ont
import time
import requests
from rdflib_sesame import SesameStore
import multiprocessing
import concurrent.futures
from shapely.wkt import dumps, loads
from shapely.geometry import Point,GeometryCollection, MultiLineString,MultiPoint,MultiPolygon
import json
from shapely.geometry import mapping, shape


ENDPOINTS = {"virtuoso": "http://localhost:8890/sparql",
             "stardog" :"http://localhost:5820/rede/query",
             "graphdb" : "http://localhost:7200/rede",
             "marmotta" : "http://localhost:8080/sparql"
             }

RES = URIRef("http://127.0.0.1:6543/atlas/mrhsa/resource/33c0f063-c1d4-4a09-8184-ca736e6aa8b7")
RES2 = URIRef("http://127.0.0.1:6543/atlas/mrhsa/resource/b49b4fcb-d95e-49f2-a499-d5907aafb5aa")
PRED = URIRef("http://127.0.0.1:6543/properties/map#werkbeschreibung")
OBS = URIRef("http://purl.org/linked-data/cube#Observation")
RES3 = URIRef("http://127.0.0.1:6543/linguistics/phonetic/NearCloseNearBackRoundedVowel")
LIT = Literal("Strodthagen")
LIT2 = Literal("bose")
BN = BNode("x151A5F8E32EN94bc7de8665f42f9aa9e561906aa60da")

CTX1 =URIRef("http://127.0.0.1:6543/geodata")
CTX2 = URIRef("http://127.0.0.1:6543/atlas/mrhsa")

SE = "http://localhost:18080/graphdb%2Dworkbench%2Dse"
FREE = "http://localhost:7200"

QU = """
#PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select ?s ?p ?o ?w ?l ?obs where {
	 ?s ?p ?o .
    OPTIONAL {?o rdfs:label ?l} .
    Optional { ?o <http://www.opengis.net/ont/geosparql#hasGeometry> [ <http://www.opengis.net/ont/geosparql#asWKT> ?w] } .
    optional {?obs <http://127.0.0.1:6543/properties#at> ?o . ?s <http://127.0.0.1:6543/properties/map#has_observation> ?obs}

    VALUES (?s) {(<http://127.0.0.1:6543/atlas/mrhsa/resource/33c0f063-c1d4-4a09-8184-ca736e6aa8b7>)}
} order by ?o ?l ?w

"""

QU2 = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdfs2: <http://www.w3.org/2000/01/rdf-schema#>
select (min(?l) as ?ml) (group_concat(distinct ?w) as ?wkt) where {
	 ?s ?p ?o .
    OPTIONAL {?o rdfs:label ?l} .
    Optional { ?o <http://www.opengis.net/ont/geosparql#hasGeometry> [ <http://www.opengis.net/ont/geosparql#asWKT> ?w] } .
    optional {?obs <http://127.0.0.1:6543/properties#at> ?o . ?s <http://127.0.0.1:6543/properties/map#has_observation> ?obs}

    #VALUES (?s) {(<http://127.0.0.1:6543/atlas/mrhsa/resource/33c0f063-c1d4-4a09-8184-ca736e6aa8b7>)}
} group by ?o order by ?o ?l ?w
"""

QU3 = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select *  from <http://127.0.0.1:6543/ontology/linguistics> from <http://127.0.0.1:6543/atlas/mrhsa>
where { ?s a/rdfs:subClassOf* <http://127.0.0.1:6543/ontology/linguistics/MS-sigle-ii> ; rdfs:label ?l }
"""

QU4 = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select *  from <http://127.0.0.1:6543/ontology/linguistics> from <http://127.0.0.1:6543/atlas/mrhsa>
where { ?s rdfs:label ?l ; a/rdfs:subClassOf* <http://127.0.0.1:6543/ontology/linguistics/MS-sigle-ii>  }
"""

QU5 = """

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
select distinct ?o where {?s <http://127.0.0.1:6543/properties/map#has_map>+/(rdfs:do|!rdfs:do)+ ?o}


"""

QU6 = """

PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?g2 ?l  WHERE {
   ?d  <http://www.opengis.net/ont/geosparql#hasGeometry> ?g1 ; rdfs:label "Hessen".
  ?g1 <http://www.opengis.net/ont/geosparql#asWKT> ?wkt1.
    ?g1 a <http://www.opengis.net/ont/sf#Polygon> .
    ?f2 <http://www.opengis.net/ont/geosparql#hasGeometry> ?g2 ; rdfs:label ?l .
  ?g2 <http://www.opengis.net/ont/geosparql#asWKT> ?wkt2.
    ?g2 a <http://www.opengis.net/ont/sf#Point> .
  filter(<http://www.opengis.net/def/function/geosparql/sfIntersects> (?wkt1, ?wkt2)) .
} #limit 2
"""

QU7 = """
PREFIX rm-ns: <http://127.0.0.1:6543/properties/map#>
PREFIX lns: <http://127.0.0.1:6543/linguistics/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX qb: <http://purl.org/linked-data/cube#>

select ?le (count(distinct ?s) as ?CMaps) (count(distinct ?obs) as ?CObs) (group_concat(distinct ?l) as ?label)  from <http://127.0.0.1:6543/atlas/mrhsa> from <http://127.0.0.1:6543/ontology/linguistics> where {
    values (?X) {(<http://127.0.0.1:6543/ontology/linguistics/MS-sigle-ii>) (<http://127.0.0.1:6543/ontology/linguistics/MS-sigle-iii>) (<http://127.0.0.1:6543/ontology/linguistics/MS-sigle-iv>)}
	?s a lns:Lautkarte ; rm-ns:has_legend_entry ?le ;rm-ns:covers_systematic/rdfs:subClassOf ?X .
    ?le rdfs:label ?l  .
    ?obs a qb:Observation; rm-ns:has_legend_entry ?le ;rm-ns:in_map ?s
} group by ?le order by Desc(?CMaps)
"""

CONST = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#Select distinct ?s ?p ?o ?ol ?feat
Construct {?s ?p ?o . ?o <http://127.0.0.1:6543/properties#at> ?feat . ?o rdfs:label ?ol }
where { ?s ?p ?o .
 ?s <http://127.0.0.1:6543/properties#uuid> '01f05526-f82c-4a11-947f-97edb1274e68' . optional{ ?o <http://127.0.0.1:6543/properties#at> ?feat } . optional {?o rdfs:label ?ol }
}
order by ?p ?o
"""
QSIMPLE = """ select * where { ?s a ?o}  limit 2 """

def st_time(func):
    """
        st decorator to calculate the total time of a func
    """

    def st_func(*args, **keyArgs):
        t1 = time.time()
        r = func(*args, **keyArgs)
        t2 = time.time()
        print ("Function={}, Time={}".format(func.__name__, t2 - t1))
        return r

    return st_func

@st_time
def access_via_rdflib(endpoint, trip):
    store = SPARQLStore(endpoint)
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = list()
    for line in ds.triples((s,p,o)):

        out.append(line)

    return out

@st_time
def access_via_rdflib_sesame(trip, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    print(len(ds))
    s,p,o = trip
    out = 0
    for line in ds.triples((s,p,o)):
        #print(line[0].n3())
        #out.append(line)
        #print(line)
        out =out+1

    return out

@st_time
def nested_rdflib(endpoint, trip):
    store = SPARQLStore(endpoint)
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = 0
    for a,b,c in ds.triples((s,p,o)):
        if isinstance(c,URIRef):
            for j in ds.triples((c, None, None)):
                out = out+1


    return out

@st_time
def nested_rdflib_sesame(trip, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = list()
    #print (CTX2.n3())
    G = Graph()
    for a,b,c in ds.triples((s,p,o)):
        #G.add((a,b,c))
        if isinstance(c, URIRef):
            for j in ds.triples((c, None, None)):
                out.append(j)
                #print(j)

    return out

@st_time
def nested_virtuoso(trip):
    Virtuoso = plugin("Virtuoso", Store)
    store = Virtuoso("DSN=VOS;UID=dba;PWD=dba;WideAsUTF16=Y")
    #store = SesameStore("http://localhost:18080/graphdb%2Dworkbench%2Dfree", "rede")
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = list()
    #print (CTX2.n3())
    for a,b,c in ds.triples((s,p,o)):
        if isinstance(c, URIRef):
            for j in ds.triples((c, None, None)):
                out.append(j)
                #print(j)

    return out

@st_time
def nested_rdflib2(endpoint, trip):
    store = SPARQLStore(endpoint)
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = list()
    for a,b,c in ds.triples((s,p,o)):
        if isinstance(c,URIRef):
            for j1, j2, j3 in ds.triples((c, None, None)):
                if isinstance(j3,URIRef):
                    for i in ds.triples((j3, None,None)):
                        out.append(i)
    return out




@st_time
def nested_rdflib_sesame2(trip, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)

    s,p,o = trip
    out = list()
    for a,b,c in ds.triples((s,p,o)):
        if isinstance(c,URIRef):
            for j1, j2, j3 in ds.triples((c, None, None)):
                if isinstance(j3,URIRef):
                    for i in ds.triples((j3, None,None)):
                        out.append(i)
    return out

@st_time
def walk_via_multiprocessing(trip, con=FREE):



    pool = multiprocessing.Pool(8)
    #out1, out2, out3 = zip(*pool.map(calc_stuff, range(0, 10 * offset, offset)))
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    s,p,o = trip
    gg = [(o,ds, CTX2) for s,p,o in ds.triples((s, p, o)) if isinstance(o, URIRef)]
    out = pool.map(walk2, gg)
    pool.close()
    pool.join()
    return [item for sublist in out for item in sublist]

def walk(g):
    o, ds, ctx = g
    #store = SesameStore("http://localhost:18080/graphdb%2Dworkbench%2Dfree", "rede")
    #ds = Dataset(store, default_union=True)
    l = list()
    pool = multiprocessing.Pool(16)
    gg = [(x3,ds, CTX2) for x1,x2,x3 in ds.triples((o, None, None)) if isinstance(o, URIRef)]
    out = pool.map(walk2, gg)
    pool.close()
    pool.join()
    return [item for sublist in out for item in sublist]


def walk2(g):
    o, ds, ctx = g
    l = list()
    for x1,x2,x3 in ds.triples((o, None, None)):
        l.append( x1)
    return l

@st_time
def walk_with_future(trip, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    s,p,o = trip
    out = list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tt = {executor.submit(ds.triples, t) : t for t in ds.triples( (s,p,o) ) }
        #print(tt)
        #future_to_url = { executor.submit(ds.triples, (t) ):  t for ds.triples((s,p,o)}
        for future in concurrent.futures.as_completed(tt):
            res = future.result()
            tt = {executor.submit(ds.triples, (None, None, t[2])) :t  for t in res  }
            for future2 in concurrent.futures.as_completed(tt):
                res2 = future2.result()

                out.append(o for o in res2)
            #for i in res:
                #print(i)
    return out

@st_time
def walk_context(trip, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    ds1 = ds.get_context(URIRef("http://127.0.0.1:6543/geodata"))
    ds2 = ds.get_context(URIRef("http://127.0.0.1:6543/atlas/mrhsa"))
    ds = ds1+ds2
    s,p,o = trip
    out = list()
    for line in ds.triples((s,p,o)):
        out.append(line)

    return out



@st_time
def query_rdflib_sesame(qy, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    out = list()
    for r in ds.query(qy):
        out.append(r)
    return out


@st_time
def query_sparql_store(endpoint, qy):
    store = SPARQLStore(endpoint)
    ds = Dataset(store, default_union=True)
    out = list()
    for r in ds.query(qy, initBindings={"s": RES}):
        out.append(r)
    return out


def query_rdflib_sesame2(qy, con=FREE):
    store = SesameStore(con, "rede")
    ds = Dataset(store, default_union=True)
    #out = list()
    return ds.query(qy, initBindings={"s": RES})


@st_time
def build_geojson(res):
    m = dict(type="Map")
    fc = dict(type="FeatureCollection")
    f = []
    for out in res:
        #print (out["w"])
        if out["o"] and out["l"] and out["w"]:
            f.append({"type": "Feature", "geometry": set_geometry(out["w"]), "properties":
                {"label": str(out["l"]), "obs": str(out["obs"])}})
        elif out["p"] and out["o"] and out["l"]:
            m.setdefault(str(out["p"]),[]).append({str(out["o"]) : str(out["l"])})


    fc["features"] = f
    m["geodata"] = fc
    return json.dumps(m)

def set_geometry(wkt):
    return mapping(loads(wkt))



if __name__ == "__main__":

    #print (len(access_via_rdflib(ENDPOINTS["graphdb"], (None, None, OBS))))
    #print (len(access_via_rdflib_sesame((None, None, None), FREE)))
    print (access_via_rdflib_sesame((None, None, LIT), FREE))
    #print (len(nested_rdflib(ENDPOINTS["graphdb"], (RES, None, None))))
    #print (len(nested_rdflib_sesame((RES2, None, None), FREE)))
    #print (len(nested_virtuoso((RES, None, None))))
    #print(len(nested_rdflib_sesame2((RES, None, None))))
    #print(len(nested_rdflib2(ENDPOINTS["graphdb"], (RES, None, None))))
    #print (len(walk_via_multiprocessing((RES, None, None), FREE)))
    #print(len(walk_with_future((RES, None, None), FREE)))
    #print(len(walk_context((RES, None, None))))
    #print(len(query_sparql_store(ENDPOINTS["graphdb"], QU)))
    #print(len(query_sparql_store(ENDPOINTS["graphdb"], QU)))
    print("const")
    print(len(query_rdflib_sesame(CONST, FREE)))
    print("select")
    print(len(query_rdflib_sesame(QU, FREE)))
    #print(len(query_rdflib_sesame(QU3, FREE)))
    #print(len(query_rdflib_sesame(QU4, FREE)))
    #print(len(query_rdflib_sesame(QU7, FREE)))
    #print(len(query_rdflib_sesame(QU6, FREE)))
    #print(len(query_rdflib_sesame(QU5, FREE)))
    #print(len(query_sparql_store(ENDPOINTS["graphdb"], QU)))
    #print(build_geojson(query_rdflib_sesame2(QU, FREE)))


