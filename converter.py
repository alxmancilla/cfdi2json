#!/usr/bin/env python3
# Python 3 only

from xmljson import GData
from xml.etree.ElementTree import fromstring
import pymongo
from pymongo import InsertOne, ReplaceOne
from pymongo.errors import BulkWriteError, ConnectionFailure, OperationFailure
import json
import fnmatch
import os
import datetime
import re
import sys

def load_env(path):
    # load KEY=VALUE pairs from a .env file without overriding existing environment variables
    if not os.path.isfile(path):
        return
    with open(path) as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))

load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

URI_STRING = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGODB_DATABASE", "demo")
COLLECTION_NAME = os.environ.get("MONGODB_COLLECTION", "cfdi")

DATADIR = os.environ.get("DATADIR", "./xmls")

BATCH_SIZE = 500
UUID_FIELD = "Comprobante.Complemento.TimbreFiscalDigital.UUID"

VALUE = "$t"
EMPTY = "\""
regex = re.compile(r"\"\{http.*\w\}")
# only plain numbers become int/float; codes with leading zeros, exponents or more than 15 digits stay strings
NUMBER = re.compile(r"^-?(0|[1-9][0-9]{0,14})(\.[0-9]+)?$")
DATE_FORMATS = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f")
NOMINA_STRING_KEYS = {
    "Banco", "Clave", "CuentaBancaria", "NumEmpleado", "NumSeguridadSocial",
    "PeriodicidadPago", "RegistroPatronal", "RiesgoPuesto", "TipoContrato",
    "TipoDeduccion", "TipoHoras", "TipoIncapacidad", "TipoJornada",
    "TipoOtroPago", "TipoPercepcion", "TipoRegimen",
}


def parse_value(value):
    if value is None or not NUMBER.match(value):
        return value
    if "." in value:
        return float(value)
    return int(value)


bf = GData(xml_fromstring=parse_value)


def parse_date(value):
    for date_format in DATE_FORMATS:
        try:
            return datetime.datetime.strptime(value, date_format)
        except ValueError:
            pass
    print("Warning: unrecognized Fecha format, stored as string:", value)
    return value


def stringify_keys(node, keys):
    # convert numeric values of the given keys to strings at any depth
    if isinstance(node, list):
        for item in node:
            stringify_keys(item, keys)
    elif isinstance(node, dict):
        for key, value in node.items():
            if key in keys and isinstance(value, (int, float)):
                node[key] = str(value)
            else:
                stringify_keys(value, keys)


def traverse_directories(conn, base_dir, batch):
    print("directory: ", base_dir)
    list_files = os.listdir(base_dir)
    for a in list_files:
            file_name = os.path.join(base_dir, a)
            if os.path.isdir(file_name):
                traverse_directories(conn, file_name, batch)
            elif fnmatch.fnmatch(a, '*.xml'):
                try:
                    document = convertXML2JSON(file_name)
                    document = coerceDataTypes(document)
                except Exception as e:
                    print("Exception filename", file_name)
                    print("Exception e: ", e)
                    continue
                batch.append((file_name, document))
                if len(batch) >= BATCH_SIZE:
                    write_documents(conn, batch)
                    batch.clear()


def coerceDataTypes(document):
    comprobante = document["Comprobante"]
    for item in comprobante:
        if item == "NoCertificado":
            comprobante["NoCertificado"] = str(comprobante["NoCertificado"])
        elif item == "Fecha":
            comprobante["Fecha"] = parse_date(str(comprobante["Fecha"]))
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

                if compitem == "Nomina":
                    stringify_keys(comprobante["Complemento"]["Nomina"], NOMINA_STRING_KEYS)

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
    # open the XML file, convert to json and return it as an object
    with open(file, "rb") as input:
        jsonOut = bf.data(fromstring(input.read()))
        jsonString = json.dumps(jsonOut, indent=2)

        result = regex.sub(EMPTY, jsonString)
        result = result.replace(VALUE, "value")

    return json.loads(result)


def get_uuid(doc):
    complementos = doc["Comprobante"].get("Complemento")
    if not isinstance(complementos, list):
        complementos = [complementos]
    for complemento in complementos:
        if isinstance(complemento, dict) and isinstance(complemento.get("TimbreFiscalDigital"), dict):
            return complemento["TimbreFiscalDigital"].get("UUID")
    return None


def ensure_indexes(conn):
    collection = conn[DB_NAME][COLLECTION_NAME]
    try:
        collection.create_index(UUID_FIELD, unique=True,
                                partialFilterExpression={UUID_FIELD: {"$exists": True}})
    except OperationFailure as e:
        print("Warning: could not create unique index on %s: %s" % (UUID_FIELD, e))


def write_documents(conn, batch):
    # upsert stamped CFDIs by UUID so re-runs do not create duplicates
    collection = conn[DB_NAME][COLLECTION_NAME]
    requests = []
    for file_name, doc in batch:
        uuid = get_uuid(doc)
        if uuid:
            requests.append(ReplaceOne({UUID_FIELD: uuid}, doc, upsert=True))
        else:
            requests.append(InsertOne(doc))
    try:
        result = collection.bulk_write(requests, ordered=False)
        print("batch written: %d new without UUID, %d new by UUID, %d existing replaced"
              % (result.inserted_count, result.upserted_count, result.matched_count))
    except BulkWriteError as e:
        for error in e.details["writeErrors"]:
            print("¡¡¡ERROR!!!!", error["errmsg"])
            print("filename: " + batch[error["index"]][0])
    except Exception as e:
        print("¡¡¡ERROR!!!!", e)
        print("files in failed batch:", [file_name for file_name, _ in batch])


if __name__ == "__main__":
    print("This is the name of the script: ", sys.argv[0])
    if len(sys.argv) > 1:
        print("This is the argument of the script: ", sys.argv[1])
        DATADIR = sys.argv[1]
    conn = None
    try:
        conn = pymongo.MongoClient(URI_STRING, serverSelectionTimeoutMS=5000)
        conn.admin.command("ping")
        print("Connected to MongoDB")
        ensure_indexes(conn)
        start_time = datetime.datetime.utcnow()
        print("start time: ", start_time)
        batch = []
        traverse_directories(conn, DATADIR, batch)
        if batch:
            write_documents(conn, batch)
        end_time = datetime.datetime.utcnow()
        print("end time: ", end_time)
        print("%.2f seconds" % (end_time - start_time).total_seconds())
        print("Operation completed successfully!!!")
    except ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
    finally:
        if conn is not None:
            conn.close()
