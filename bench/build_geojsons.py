__author__ = 'robert'

from shapely.wkt import dumps, loads
from shapely.geometry import Point,GeometryCollection, MultiLineString,MultiPoint,MultiPolygon
import json
from shapely.geometry import mapping, shape

PWKT1 = "POINT(2.551 51.016)"
PWKT2 = "POINT(2.597 50.877)"
LWKT1 = "LINESTRING(5.33107 51.96296,5.31031 51.9596,5.25416 51.97901,5.23915 51.977,5.2212 51.96833,5.19728 51.96552,5.17274 51.9676,5.15394 51.97388,5.14454 51.98115,5.13063 51.99592,5.11842 52.00184,5.10475 52.00373,5.09547 52.0008,5.08656 51.99659,5.06373 51.99274,5.05482 51.98847,5.04029 51.977,5.03016 51.97425,4.98878 51.98011,4.9867 51.97852,4.9546 51.96705,4.92652 51.94916,4.91651 51.9466,4.85096 51.94916,4.83058 51.9466,4.77613 51.92237,4.74012 51.91602,4.68092 51.89814,4.59498 51.89814,4.57167 51.90137,4.52796 51.91547,4.50904 51.91865,4.46119 51.90559,4.34083 51.89564,4.30885 51.89826,4.27699 51.91224,4.23817 51.91865,4.1277 51.98212,4.0984 51.98908)"
LWKT2 = "LINESTRING(4.70765 49.76734,4.70143 49.76734)"
PgWKT1 = "POLYGON((15 50.83333,15.25 50.83333,15.25 51,15 51,15 50.83333))"
PgWKT2 = "POLYGON((6.009 51,6.17567 51,6.17567 51.08333,6.009 51.08333,6.009 51))"

x = GeometryCollection
a = []
t = set()
for i in PgWKT2,LWKT2,LWKT1:
    g = loads(i)
    print(g.geom_type)
    t.add(str(g.geom_type).upper())
    a.append(g)

print(t)
if len(t)>1:
    x = GeometryCollection(a)
elif "POINT" in t:
    x = MultiPoint(a)
elif "POLYGON" in t:
    x = MultiPolygon(a)
elif "LINESTRING" in t:
    x = MultiLineString(a)
else:
    raise "Unsupported geometry"
#x = MultiPolygon(a)
#print(x)

y =mapping(x)
print(y)