#!/usr/bin/env python3
# Python 3 only

from collections import Counter
from decimal import Decimal, DecimalException
from xml.etree.ElementTree import fromstring
from bson.decimal128 import Decimal128
import pymongo
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError, CollectionInvalid, ConnectionFailure, OperationFailure
import datetime
import fnmatch
import hashlib
import os
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
SIGNATURES_COLLECTION_NAME = os.environ.get("MONGODB_SIGNATURES_COLLECTION", COLLECTION_NAME + "_signatures")

DATADIR = os.environ.get("DATADIR", "./xmls")

BATCH_SIZE = 500
SCHEMA_VERSION = 3
TEXT_KEY = "value"
DATE_PREFIX = "Fecha"
DATE_FORMATS = ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d")

# attribute values are stored as strings, except amounts (Decimal128), counters (int) and dates
DECIMAL_PREFIX = "Total"
DECIMAL_KEYS = {
    "Base", "Cantidad", "Descuento", "ImpPagado", "ImpSaldoAnt", "ImpSaldoInsoluto",
    "Importe", "ImporteExento", "ImporteGravado", "ImportePagado", "IngresoAcumulable",
    "IngresoNoAcumulable", "Monto", "MontoDiario", "NumDiasPagados", "SalarioBaseCotApor",
    "SalarioDiarioIntegrado", "SubTotal", "SubsidioCausado", "TasaOCuota", "TipoCambio",
    "TipoCambioP", "ValorUnitario",
}
INT_KEYS = {"NumParcialidad"}

# CFDI elements that may repeat are always stored as arrays
ARRAY_KEYS = {
    "CfdiRelacionado", "Concepto", "ConceptoConsumoDeCombustibles",
    "ConceptoEstadoDeCuentaCombustible", "Deduccion", "Determinado", "DoctoRelacionado",
    "HorasExtra", "Incapacidad", "InformacionAduanera", "Nomina", "OtroPago", "Pago",
    "Pagos", "Parte", "Percepcion", "Retencion", "SubContratacion", "Traslado",
}

# certificates, digital seals and payment chains are large and rarely read; stored in the signatures collection
SIGNATURE_KEYS = {"CadPago", "CertPago", "Certificado", "Sello", "SelloCFD", "SelloPago", "SelloSAT"}

STRING = {"bsonType": "string"}
DECIMAL = {"bsonType": "decimal"}
DATE = {"bsonType": "date"}
PARTY = {"bsonType": "object", "required": ["Rfc"], "properties": {"Rfc": STRING}}
VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["schemaVersion", "source", "Comprobante"],
        "properties": {
            "schemaVersion": {"bsonType": "int", "enum": [SCHEMA_VERSION]},
            "source": {
                "bsonType": "object",
                "required": ["file", "importedAt"],
                "properties": {"file": STRING, "importedAt": DATE},
            },
            "Comprobante": {
                "bsonType": "object",
                "required": ["Version", "Fecha", "SubTotal", "Total", "Emisor", "Receptor", "Conceptos"],
                "properties": {
                    "Version": STRING,
                    "Fecha": DATE,
                    "SubTotal": DECIMAL,
                    "Total": DECIMAL,
                    "Emisor": PARTY,
                    "Receptor": PARTY,
                    "Conceptos": {
                        "bsonType": "object",
                        "required": ["Concepto"],
                        "properties": {"Concepto": {"bsonType": "array", "minItems": 1}},
                    },
                    "Complemento": {
                        "bsonType": "object",
                        "properties": {
                            "TimbreFiscalDigital": {
                                "bsonType": "object",
                                "required": ["UUID", "FechaTimbrado"],
                                "properties": {"UUID": STRING, "FechaTimbrado": DATE},
                            },
                        },
                    },
                },
            },
        },
    },
}
SIGNATURES_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["schemaVersion", "Comprobante"],
        "properties": {
            "schemaVersion": {"bsonType": "int", "enum": [SCHEMA_VERSION]},
            "Comprobante": {"bsonType": "object"},
        },
    },
}


def local_name(name):
    # drop the XML namespace, e.g. {http://www.sat.gob.mx/cfd/3}Comprobante -> Comprobante
    return name.rsplit("}", 1)[-1]


def parse_date(key, value):
    for date_format in DATE_FORMATS:
        try:
            return datetime.datetime.strptime(value, date_format)
        except ValueError:
            pass
    print("Warning: unrecognized date in %s, stored as string: %s" % (key, value))
    return value


def convert_value(key, value):
    if key in DECIMAL_KEYS or key.startswith(DECIMAL_PREFIX):
        try:
            number = Decimal(value)
            if number.is_finite():
                return Decimal128(number)
        except DecimalException:
            pass
        print("Warning: invalid amount in %s, stored as string: %s" % (key, value))
        return value
    if key in INT_KEYS:
        try:
            return int(value)
        except ValueError:
            print("Warning: invalid integer in %s, stored as string: %s" % (key, value))
            return value
    if key.startswith(DATE_PREFIX):
        return parse_date(key, value)
    return value


def element_to_dict(element):
    node = {}
    for name, value in element.attrib.items():
        key = local_name(name)
        node[key] = convert_value(key, value)
    if element.text and element.text.strip():
        node[TEXT_KEY] = element.text
    children = [child for child in element if isinstance(child.tag, str)]
    counts = Counter(local_name(child.tag) for child in children)
    for child in children:
        key = local_name(child.tag)
        value = element_to_dict(child)
        if key in ARRAY_KEYS or counts[key] > 1:
            items = node.get(key)
            if not isinstance(items, list):
                items = node[key] = []
            items.append(value)
        else:
            node[key] = value
    return node


def extract_signatures(node):
    # remove signature fields from node and return them in the same nested shape (None if there are none)
    if isinstance(node, list):
        extracted = [extract_signatures(item) or {} for item in node]
        return extracted if any(extracted) else None
    if not isinstance(node, dict):
        return None
    extracted = {}
    for key in list(node):
        if key in SIGNATURE_KEYS:
            extracted[key] = node.pop(key)
        else:
            value = extract_signatures(node[key])
            if value:
                extracted[key] = value
    return extracted or None


def get_timbre(document):
    complemento = document["Comprobante"].get("Complemento")
    if isinstance(complemento, dict) and isinstance(complemento.get("TimbreFiscalDigital"), dict):
        return complemento["TimbreFiscalDigital"]
    return None


def convert_file(file_name):
    with open(file_name, "rb") as input:
        content = input.read()
    root = fromstring(content)
    if local_name(root.tag) != "Comprobante":
        raise ValueError("root element is %s, expected Comprobante" % local_name(root.tag))
    document = {"Comprobante": element_to_dict(root)}
    timbre = get_timbre(document)
    if timbre and isinstance(timbre.get("UUID"), str):
        # SAT UUIDs are case-insensitive; normalize so the same invoice always maps to the same _id
        timbre["UUID"] = timbre["UUID"].upper()
        document["_id"] = timbre["UUID"]
    else:
        # unstamped CFDI: identify by file content so re-runs replace instead of duplicating
        document["_id"] = "sha256:" + hashlib.sha256(content).hexdigest()
    document["schemaVersion"] = SCHEMA_VERSION
    document["source"] = {"file": file_name, "importedAt": datetime.datetime.now(datetime.timezone.utc)}
    signatures = extract_signatures(document["Comprobante"])
    if signatures:
        signatures = {"_id": document["_id"], "Comprobante": signatures, "schemaVersion": SCHEMA_VERSION}
    return document, signatures


def import_directory(db, base_dir):
    stats = Counter()
    batch = []
    for directory, subdirs, files in os.walk(base_dir):
        subdirs.sort()
        print("directory: ", directory)
        for name in sorted(fnmatch.filter(files, "*.xml")):
            file_name = os.path.join(directory, name)
            try:
                document, signatures = convert_file(file_name)
            except Exception as e:
                print("Exception filename", file_name)
                print("Exception e: ", e)
                stats["failed"] += 1
                continue
            batch.append((file_name, document, signatures))
            if len(batch) >= BATCH_SIZE:
                stats.update(write_batch(db, batch))
                batch = []
    if batch:
        stats.update(write_batch(db, batch))
    return stats


def ensure_collection(db, name, validator):
    # $jsonSchema validator rejects documents with missing core fields or wrong types
    options = {"validator": validator, "validationLevel": "strict", "validationAction": "error"}
    try:
        db.create_collection(name, **options)
        print("created collection %s with schema validation" % name)
    except CollectionInvalid:
        try:
            db.command("collMod", name, **options)
        except OperationFailure as e:
            print("Warning: could not update schema validation on %s: %s" % (name, e))
    except OperationFailure as e:
        print("Warning: could not create collection %s: %s" % (name, e))


def ensure_collections(db):
    ensure_collection(db, COLLECTION_NAME, VALIDATOR)
    ensure_collection(db, SIGNATURES_COLLECTION_NAME, SIGNATURES_VALIDATOR)
    # documents imported before schema version 2 use ObjectId _ids; this lookup is served by the _id index
    if db[COLLECTION_NAME].find_one({"_id": {"$type": "objectId"}}, {"_id": 1}):
        print("Warning: %s contains documents from an earlier schema version; "
              "drop the collection and re-import to avoid duplicates" % COLLECTION_NAME)


def write_documents(collection, items):
    # replace by _id: a re-imported invoice overwrites its previous version
    # items: list of (file_name, document); returns (stats, indexes of failed items)
    requests = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for _, doc in items]
    try:
        result = collection.bulk_write(requests, ordered=False)
        details = {"nUpserted": result.upserted_count, "nMatched": result.matched_count, "writeErrors": []}
    except BulkWriteError as e:
        details = e.details
        for error in details["writeErrors"]:
            print("¡¡¡ERROR!!!!", error["errmsg"])
            print("filename: " + items[error["index"]][0])
    except Exception as e:
        print("¡¡¡ERROR!!!!", e)
        print("files in failed batch:", [file_name for file_name, _ in items])
        return Counter(failed=len(items)), set(range(len(items)))
    stats = Counter(inserted=details["nUpserted"], replaced=details["nMatched"],
                    failed=len(details["writeErrors"]))
    return stats, {error["index"] for error in details["writeErrors"]}


def write_batch(db, batch):
    stats, failed = write_documents(db[COLLECTION_NAME], [(f, doc) for f, doc, _ in batch])
    print("batch written: %(inserted)d inserted, %(replaced)d replaced, %(failed)d failed" % stats)
    # signatures are written only for invoices that were stored, so no orphan signature documents are created
    signatures = [(f, sig) for i, (f, _, sig) in enumerate(batch) if sig and i not in failed]
    if signatures:
        signature_stats, _ = write_documents(db[SIGNATURES_COLLECTION_NAME], signatures)
        stats["signatures_failed"] = signature_stats["failed"]
    return stats


if __name__ == "__main__":
    print("This is the name of the script: ", sys.argv[0])
    if len(sys.argv) > 1:
        print("This is the argument of the script: ", sys.argv[1])
        DATADIR = sys.argv[1]
    if not os.path.isdir(DATADIR):
        sys.exit("Directory not found: %s" % DATADIR)
    conn = None
    try:
        # single-threaded batch job: the default pool is sufficient; fail fast if the server is unreachable
        conn = pymongo.MongoClient(URI_STRING, appname="cfdi2json", serverSelectionTimeoutMS=5000)
        conn.admin.command("ping")
        print("Connected to MongoDB")
        db = conn[DB_NAME]
        ensure_collections(db)
        start_time = datetime.datetime.now(datetime.timezone.utc)
        print("start time: ", start_time)
        stats = import_directory(db, DATADIR)
        end_time = datetime.datetime.now(datetime.timezone.utc)
        print("end time: ", end_time)
        print("%.2f seconds" % (end_time - start_time).total_seconds())
        print("summary: %d inserted, %d replaced, %d failed, %d signature writes failed"
              % (stats["inserted"], stats["replaced"], stats["failed"], stats["signatures_failed"]))
        ok = not stats["failed"] and not stats["signatures_failed"]
        print("Operation completed successfully!!!" if ok else "Operation completed with errors")
    except ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
    finally:
        if conn is not None:
            conn.close()
