#!/usr/bin/env python3
# Python 3 only

from xmljson import gdata as bf
from xml.etree.ElementTree import fromstring
import pymongo
import json
import fnmatch
import os
import datetime
import re
import sys
from builtins import isinstance

URI_STRING ="mongodb://localhost:27017/demo"

DATADIR = "/home/ec2-user/xmls"


VALUE = "$t"
EMPTY = "\""
regex = re.compile(r"\"\{http.*\w\}")
file_name = ""

def traverse_directories(conn, base_dir):
    print("directory: ", base_dir)
    list_files = os.listdir(base_dir)
    for a in list_files:
            file_name = base_dir +'/'+ a
            #print(abs_path)
            if os.path.isdir(file_name):
                traverse_directories(conn, file_name)
            elif fnmatch.fnmatch(a, '*.xml'):
                try: 
                    document = convertXML2JSON(file_name)
                    document = coerceDataTypes(document)  
                    write_document(conn, document, file_name)
                except Exception as e:
                    print("Exception filename", file_name)
                    print("Exception e: ", e)
                    continue
   

def coerceDataTypes(document):
    comprobante = document["Comprobante"]
    for item in comprobante:
        if item == "NoCertificado":
            comprobante["NoCertificado"] = str(comprobante["NoCertificado"])
        elif item == "Fecha":
            comprobante["Fecha"] = datetime.datetime.strptime(comprobante["Fecha"], "%Y-%m-%dT%H:%M:%S")
        elif item == "Folio":
            comprobante["Folio"] = str(comprobante["Folio"])
        elif item == "Serie":
            comprobante["Serie"] = str(comprobante["Serie"])
        elif item == "CuentaPredial":
            comprobante["CuentaPredial"]["Numero"] = str(comprobante["CuentaPredial"]["Numero"])
        elif item == "Complemento":
            #print("Complemento: ... ", comprobante["Complemento"])
            for compitem in comprobante["Complemento"]:
                if compitem == "CFDIRegistroFiscal":
                    comprobante["Complemento"]["CFDIRegistroFiscal"]["Folio"] = str(comprobante["Complemento"]["CFDIRegistroFiscal"]["Folio"])
                if compitem == "TimbreFiscalDigital":
                    comprobante["Complemento"]["TimbreFiscalDigital"]["NoCertificadoSAT"] = str(comprobante["Complemento"]["TimbreFiscalDigital"]["NoCertificadoSAT"])

                if compitem == "ConsumoDeCombustibles":
                    #print("ConsumoDeCombustibles: ... ", comprobante["Complemento"]["ConsumoDeCombustibles"])
                    cdc = comprobante["Complemento"]["ConsumoDeCombustibles"]
                    if isinstance(cdc, dict):
                        for keys in cdc:
                            if keys == "Conceptos":
                                conceptos = comprobante["Complemento"]["ConsumoDeCombustibles"]["Conceptos"]
                                for ceccItem in conceptos["ConceptoConsumoDeCombustibles"]:
                                    if isinstance(ceccItem, dict):
                                        for keys in ceccItem:
                                            if keys == "Identificador":
                                                #print("Identificador ...")
                                                ceccItem["Identificador"] = str(ceccItem["Identificador"])
                                            elif keys == "identificador": 
                                                ceccItem["identificador"] = str(ceccItem["identificador"])
                                            elif keys == "Determinados": 
                                                determinados = ceccItem["Determinados"]                     
                    

                if compitem == "EstadoDeCuentaCombustible":
                    edcc = comprobante["Complemento"]["EstadoDeCuentaCombustible"]
                    if isinstance(edcc, dict):
                        for keys in edcc:
                            if keys == "Conceptos":
                                conceptos = edcc["Conceptos"]
                                for ceccItem in conceptos["ConceptoEstadoDeCuentaCombustible"]:
                                    if isinstance(ceccItem, dict):
                                        for keys in ceccItem:
                                            if keys == "Identificador":
                                                ceccItem["Identificador"] = str(ceccItem["Identificador"])
            
                if compitem == "Pagos":
                    if isinstance(comprobante["Complemento"]["Pagos"], dict): 
                        comprobante["Complemento"]["Pagos"] = [comprobante["Complemento"]["Pagos"]]
                    for pagos in comprobante["Complemento"]["Pagos"]:              
                        if isinstance(pagos["Pago"], dict):
                            for pagoItem in pagos["Pago"]:
                                if pagoItem == "NumOperacion":
                                    pagos["Pago"]["NumOperacion"] = str(pagos["Pago"]["NumOperacion"])
                                elif pagoItem == "CtaOrdenante":
                                    pagos["Pago"]["CtaOrdenante"] = str(pagos["Pago"]["CtaOrdenante"])
                                elif pagoItem == "CtaBeneficiario":
                                    pagos["Pago"]["CtaBeneficiario"] = str(pagos["Pago"]["CtaBeneficiario"])
                                elif pagoItem == "CadPago":
                                    pagos["Pago"]["CadPago"] = str(pagos["Pago"]["CadPago"])
                                elif pagoItem == "CertPago":
                                    pagos["Pago"]["CertPago"] = str(pagos["Pago"]["CertPago"])
                                elif pagoItem == "SelloPago":
                                    pagos["Pago"]["SelloPago"] = str(pagos["Pago"]["SelloPago"])
                                elif pagoItem == "DoctoRelacionado":
                                    if isinstance(pagos["Pago"]["DoctoRelacionado"], dict):
                                        pagos["Pago"]["DoctoRelacionado"] = [pagos["Pago"]["DoctoRelacionado"]]
                                    for docItem in pagos["Pago"]["DoctoRelacionado"]:
                                        if isinstance(docItem, dict):
                                            for keys in docItem:
                                                if keys == "Folio":
                                                    docItem["Folio"] = str(docItem["Folio"])                                                 
        elif item == "Conceptos":
            conceptos = comprobante["Conceptos"]
            if isinstance(conceptos, dict):
                conceptos = [conceptos]
            for concepto in conceptos:
                for keys in concepto["Concepto"]:
                    if keys == "NoIdentificacion":
                        concepto["Concepto"]["NoIdentificacion"] = str(concepto["Concepto"]["NoIdentificacion"])
                    elif keys == "Parte":
                        partes = concepto["Concepto"]["Parte"]
                        if isinstance(concepto["Concepto"]["Parte"], dict):
                            partes = [concepto["Concepto"]["Parte"]]
                        for parte in partes:
                            for llaves in parte:
                                if llaves == "NoIdentificacion":
                                    parte["NoIdentificacion"] = str(parte["NoIdentificacion"])
                    elif isinstance(keys, dict):
                        for llaves in keys:
                            if llaves == "NoIdentificacion":
                                keys["NoIdentificacion"] = str(keys["NoIdentificacion"])
                            elif llaves == "Parte":
                                parte = keys["Parte"]
                                for parteItem in parte:
                                    if parteItem == "NoIdentificacion":
                                        parte["NoIdentificacion"] = str(parte["NoIdentificacion"])        
        elif item == "Emisor":
            for emisorItem in comprobante["Emisor"]:
                if emisorItem == "Nombre":
                    comprobante["Emisor"]["Nombre"] = str(comprobante["Emisor"]["Nombre"])
                if emisorItem == "Rfc":
                    comprobante["Emisor"]["Rfc"] = str(comprobante["Emisor"]["Rfc"])
        elif item == "Receptor":
            for receptorItem in comprobante["Receptor"]:
                if receptorItem == "Nombre":
                    comprobante["Receptor"]["Nombre"] = str(comprobante["Receptor"]["Nombre"])
                if receptorItem == "Rfc":
                    comprobante["Receptor"]["Rfc"] = str(comprobante["Receptor"]["Rfc"])
    items = comprobante["Conceptos"]["Concepto"]
    
    if isinstance(items, dict):
        items = [items]    

    if isinstance(items, list):
        for item in items:
            if isinstance(item, dict):
                for keys in item:
                    if keys == "NoIdentificacion":
                        item["NoIdentificacion"] = str(item["NoIdentificacion"])
                    elif keys == "ClaveProdServ":
                        item["ClaveProdServ"] = str(item["ClaveProdServ"])
                    elif keys == "CuentaPredial":
                        item["CuentaPredial"]["Numero"] = str(item["CuentaPredial"]["Numero"])

    return document
    
    
def convertXML2JSON(file):
    # get file name
    #name = str.split(file, ".")[-2]
    #print(name)
    # open the XML file, convert to json and return it as an object
    with open(file, "rb") as input:
        jsonOut = bf.data(fromstring(input.read())) 
        jsonString = json.dumps(jsonOut, indent=2)   
        #jsonString = json.dumps(jsonOut)   
        
        result = regex.sub(EMPTY, jsonString)
        result = result.replace(VALUE, "value")
        
    return json.loads(result)
 
def write_document(conn, doc, file_name):
    #print("inserting employee: ", doc["emp_no"])
    try:
        collection = conn.demo.cfdi
        result = collection.insert_one(doc)
        return result
    except Exception as e:
        print("¡¡¡ERROR!!!!", e.args[0])
        print("filename: " + file_name)

        return False
   
if __name__ == "__main__":
    print("This is the name of the script: ", sys.argv[0])
    if len(sys.argv) > 1:
        print("This is the argument of the script: ", sys.argv[1])
        DATADIR = sys.argv[1]
    try:
        conn = pymongo.MongoClient(URI_STRING)
        print("Connected to MongoDB")
        start_time = datetime.datetime.utcnow()
        print("start time: ", start_time)
        traverse_directories(conn, DATADIR)
        end_time = datetime.datetime.utcnow()
        print("end time: ", end_time)
        print( (end_time - start_time), " seconds")
        print("Operation completed successfully!!!")
          
    except OverflowError as error:
        # Output expected OverflowErrors.
        print("error: %s" % error)
        print("file name: %s" % file_name)
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
    conn.close()    

    # para listar archivos en directorio os.listdir(path)
    #read_image(imagefile)
    #find_directories(DATADIR)
