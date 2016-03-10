__author__ = 'robert'

import struct
from io import BytesIO
from rdflib import URIRef, BNode, Literal


# Constants
MAGIC_NUMBER = b'BRDF'

FORMAT_VERSION = 1

#record types

NAMESPACE_DECL = 0
STATEMENT = 1
COMMENT = 2
VALUE_DECL = 3
ERROR = 126
END_OF_DATA = 127

#value types

NULL_VALUE = 0
URI_VALUE = 1
BNODE_VALUE = 2
PLAIN_LITERAL_VALUE = 3
LANG_LITERAL_VALUE = 4
DATATYPE_LITERAL_VALUE = 5
VALUE_REF = 6



class BinaryRDFParser:

    declaredValues = dict()
    namespaces = dict()

    #POSITION = 0


    def __init__(self, data, is_stream=False):

        if is_stream:
            self.stream = data
            self.stream.seek(0)

        else:
            self.stream = BytesIO()
            self.stream.write(data)
            self.stream.seek(0)


    def parse(self):
        mn= self.stream.read(4)
        if mn != MAGIC_NUMBER:
            #print ("No Magic")
            raise IOError("Bad Magic Number")
            #return

        fv, = struct.unpack("!i", self.stream.read(4))
        if  fv != FORMAT_VERSION:
            raise IOError("Wrong Version")
            #return

        try:

            while(True):
                recordType, = struct.unpack("!b", self.stream.read(1))
                if recordType == END_OF_DATA:
                    break
                elif recordType == STATEMENT:
                    yield self.readStatement()
                elif recordType == VALUE_DECL:
                    self.readValueDecl()
                elif recordType == NAMESPACE_DECL:
                    self.readNamespaceDecl()
                elif recordType == COMMENT:
                    self.readComment()
                else:
                    raise IOError("Parse Error for recordType %s", recordType)
                    break

        finally:
            self.stream.close()



    def readNamespaceDecl(self):
        prefix = self.readString()
        namespace = self.readString()
        self.namespaces[prefix] = namespace

    def readComment(self):

        comment = self.readString()

    def readValueDecl(self):

        id, = struct.unpack("!i", self.stream.read(4))
        v = self.readValue()
        self.declaredValues[id] = v

    def readStatement(self):

        subj = self.readValue()
        pred = self.readValue()
        obj = self.readValue()
        ctx = self.readValue()

        #print (subj,pred,obj, ctx)
        return (subj,pred,obj), ctx




    def readValue(self):

        valueType, = struct.unpack("!b",self.stream.read(1))
        #print(valueType)
        if valueType == NULL_VALUE:
            return None
        elif valueType == VALUE_REF:
            return self.readValueRef()
        elif valueType == URI_VALUE:
            return self.readUri()
        elif valueType == BNODE_VALUE:
            return self.readBNode()
        elif valueType == PLAIN_LITERAL_VALUE:
            return self.readPlainLiteral()
        elif valueType == LANG_LITERAL_VALUE:
            return self.readLangLiteral()
        elif valueType == DATATYPE_LITERAL_VALUE:
            return self.readDatatypeLiteral()
        else:
            return None

    def readValueRef(self):

        id, = length, = struct.unpack("!i", self.stream.read(4))
        return self.declaredValues[id]

    def readUri(self):

        return URIRef(self.readString())

    def readBNode(self):

        return BNode(self.readString())

    def readPlainLiteral(self):
        return Literal(self.readString())


    def readLangLiteral(self):
        label = self.readString()
        language = self.readString()
        return Literal(label, lang=language)



    def readDatatypeLiteral(self):


        label = self.readString()
        datatype = self.readString()

        dt = URIRef(datatype)
        return Literal(label, datatype=dt)


    def readString(self):

        #print(x)
        length, = struct.unpack("!i", self.stream.read(4))
        stringBytes = length << 1

        string = self.stream.read(stringBytes).decode("utf-16be")
        #print (string)
        return string




if __name__ == "__main__":

    """
    foo
"""
    f = open("bintest.brf", "rb")

    for i in  BinaryRDFParser(f, is_stream=True).parse():
        print(i)

    #readString(data)