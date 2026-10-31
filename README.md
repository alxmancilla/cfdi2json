# cfdi2json

Converts Mexican electronic invoices (CFDI XML files) into JSON documents and loads them into MongoDB.

## How it works

`converter.py` performs the following steps:

1. Recursively walks a directory and picks up every `*.xml` file whose root element is `cfdi:Comprobante`.
2. Converts each XML element into a document: attributes and child elements become fields, XML namespace prefixes are removed (e.g. `{http://www.sat.gob.mx/cfd/3}Comprobante` becomes `Comprobante`), and element text is stored in a `value` field.
3. Moves the certificate and digital seal fields into a separate signature document (see [Signatures](#signatures)).
4. Writes the documents to the configured collections (`demo.cfdi` and `demo.cfdi_signatures` by default) in batches of 500, replacing any existing document with the same `_id`. Running the script again on the same files therefore updates invoices instead of duplicating them.

Files that fail to parse, convert, or pass schema validation are reported on the console with their file name and skipped. The run ends with a summary of inserted, replaced, and failed files.

## Document model (schema version 3)

```javascript
{
  _id: "D6657733-57A1-492F-905F-ED7FFC19E486",  // TimbreFiscalDigital.UUID (uppercase)
  Comprobante: {
    Version: "3.3",
    Fecha: ISODate("2018-09-30T02:37:08Z"),
    FormaPago: "03",
    SubTotal: NumberDecimal("0.01"),
    Total: NumberDecimal("0.00"),
    Emisor: { Rfc: "...", Nombre: "...", RegimenFiscal: "601" },
    Receptor: { Rfc: "...", UsoCFDI: "P01" },
    Conceptos: { Concepto: [ { ClaveProdServ: "84121500", Importe: NumberDecimal("0.01"), ... } ] },
    Impuestos: { Traslados: { Traslado: [ { Impuesto: "002", TasaOCuota: NumberDecimal("0.000000"), ... } ] } },
    Complemento: { TimbreFiscalDigital: { UUID: "D6657733-...", FechaTimbrado: ISODate("...") } }
  },
  schemaVersion: 3,
  source: { file: "./xmls/000a0e9a-....xml", importedAt: ISODate("...") }
}
```

### Data types

| Fields | Stored as | Notes |
|--------|-----------|-------|
| Amounts: `SubTotal`, `Total`, `Descuento`, `Importe`, `Base`, `TasaOCuota`, `ValorUnitario`, `Cantidad`, `ImpPagado`, `Monto`, all `Total*` fields, and payroll amounts | `Decimal128` | Exact decimal arithmetic for sums; trailing zeros are preserved |
| `NumParcialidad` | `int` | |
| All `Fecha*` fields (`Fecha`, `FechaTimbrado`, `FechaPago`, ...) | `date` | Formats `YYYY-MM-DDTHH:MM:SS[.ffffff]` and `YYYY-MM-DD` |
| Everything else (`Rfc`, `NoCertificado`, `Folio`, `FormaPago`, `LugarExpedicion`, `ClaveProdServ`, payroll codes, ...) | `string` | Keeps leading zeros and gives every field a single type across documents |

CFDI dates have no time zone; they are stored as-is and appear as UTC in MongoDB.

A value that cannot be converted (e.g. `Total="12,34"`) is kept as a string with a warning; schema validation then rejects the document.

### Arrays

Elements that can repeat in a CFDI are always stored as arrays, even when a file contains only one: `Concepto`, `Traslado`, `Retencion`, `CfdiRelacionado`, `Parte`, `InformacionAduanera`, `Pagos`, `Pago`, `DoctoRelacionado`, `Nomina`, `Percepcion`, `Deduccion`, `OtroPago`, `Incapacidad`, `HorasExtra`, `SubContratacion`, and the fuel complement concepts. Queries and `$unwind` work the same way for every document.

### Identity

- Stamped CFDIs use the `TimbreFiscalDigital.UUID` (uppercase) as `_id`. The UUID is the invoice's unique fiscal identifier, so no extra index is needed to prevent duplicates or to look up an invoice by UUID.
- Unstamped CFDIs use `sha256:<hash of the file content>` as `_id`, so re-importing the same file replaces it.

### Signatures

Certificates, digital seals, and payment chains (`Certificado`, `Sello`, `SelloCFD`, `SelloSAT`, `CertPago`, `SelloPago`, `CadPago`) are base64 strings of several kilobytes that are only needed for audits. They are removed from the invoice and stored in the signatures collection under the same `_id`, keeping their original nested path:

```javascript
// demo.cfdi_signatures
{
  _id: "D6657733-57A1-492F-905F-ED7FFC19E486",
  Comprobante: {
    Certificado: "MIIG7DCCBNSg...",
    Sello: "gMDmrrzZ+iFT...",
    Complemento: { TimbreFiscalDigital: { SelloCFD: "gMDmrrzZ+iFT...", SelloSAT: "BjkneOSQwVK8..." } }
  },
  schemaVersion: 3
}
```

With the sample files, this reduces the average invoice document from about 5.2 KB to 1.9 KB (signature documents average 3.4 KB), so more invoices fit in memory and queries read less data. Signature documents are written only after their invoice was stored successfully. The two writes are not a transaction: if a signature write fails, the invoice remains stored without its signatures and the failure is reported in the summary; re-importing the file fixes it.

To read an invoice together with its signatures:

```javascript
db.cfdi.aggregate([
  { $match: { _id: "D6657733-57A1-492F-905F-ED7FFC19E486" } },
  { $lookup: { from: "cfdi_signatures", localField: "_id", foreignField: "_id", as: "signatures" } }
])
```

### Schema validation

On startup the script creates both collections with a `$jsonSchema` validator (`validationLevel: strict`, `validationAction: error`), or applies it to existing collections. The signatures validator requires `schemaVersion` and a `Comprobante` object. The invoice validator requires `schemaVersion`, `source`, and these `Comprobante` fields with the given types: `Version` (string), `Fecha` (date), `SubTotal` and `Total` (decimal), `Emisor.Rfc` and `Receptor.Rfc` (string), and a non-empty `Conceptos.Concepto` array. If a `TimbreFiscalDigital` is present, it must have a string `UUID` and a date `FechaTimbrado`.

Because of these required fields, CFDI 3.2 files (lowercase attribute names) are rejected.

## Requirements

- Python 3
- A running MongoDB instance (4.2 or later)
- Python packages listed in `requirements.txt` (`pymongo`)

```bash
pip install -r requirements.txt
```

## Configuration

Settings are read from a `.env` file in the project directory. This file is excluded from version control (see `.gitignore`), so create it from the provided template:

```bash
cp .env.example .env
```

Then edit `.env` with your values. Environment variables already set in the shell take precedence over values in `.env`.

| Variable             | `.env.example` value         | Description                                  |
|----------------------|------------------------------|----------------------------------------------|
| `MONGODB_URI`        | `mongodb://localhost:27017`  | MongoDB connection string                    |
| `MONGODB_DATABASE`   | `demo`                       | Target database                              |
| `MONGODB_COLLECTION` | `cfdi`                       | Target collection                            |
| `MONGODB_SIGNATURES_COLLECTION` | `cfdi_signatures` | Collection for certificates and seals (default: `<MONGODB_COLLECTION>_signatures`) |
| `DATADIR`            | `./xmls`                     | Directory scanned when no argument is given  |

If `.env` is missing or a variable is not set, `converter.py` uses the same defaults. A relative `DATADIR` is resolved against the current working directory.

Example override from the shell:

```bash
MONGODB_URI="mongodb+srv://<user>:<password>@<cluster>/" python3 converter.py
```

> Keep real credentials only in `.env`. When adding a new setting, also add it to `.env.example` with a placeholder value.

## Usage

```bash
python3 converter.py [path/to/xml/directory]
```

Running the script without arguments loads the bundled sample files from `./xmls`.

The script prints a summary for each batch, the start time, end time, total elapsed time in seconds, and the final totals of inserted, replaced, and failed files, plus failed signature writes. If MongoDB cannot be reached within 5 seconds, it prints `Could not connect to MongoDB` and exits.

The MongoDB client identifies itself as `cfdi2json` (`appname`), so its connections and operations are easy to find in server logs, `$currentOp`, and Atlas monitoring. The import runs in a single thread and uses one connection, so the driver's default pool settings are kept.

## Sample data

The `xmls/` directory contains 24 sample CFDI 3.3 invoices, including payment (`pago10`), payroll (`nomina12`), and donation (`donat`) complements.

## Example queries

```javascript
use demo

// Look up an invoice by UUID (uses the _id index)
db.cfdi.findOne({ _id: "D6657733-57A1-492F-905F-ED7FFC19E486" })

// Invoices issued since 2018-10-01
db.cfdi.find(
  { "Comprobante.Fecha": { $gte: ISODate("2018-10-01") } },
  { "Comprobante.Emisor.Rfc": 1, "Comprobante.Total": 1 }
)

// Exact totals per CFDI type (I = income, N = payroll, P = payment)
db.cfdi.aggregate([
  { $group: { _id: "$Comprobante.TipoDeComprobante", total: { $sum: "$Comprobante.Total" }, count: { $sum: 1 } } }
])
```

No secondary indexes are created. Add indexes once the query patterns are known, for example `{ "Comprobante.Emisor.Rfc": 1, "Comprobante.Fecha": -1 }` for "invoices of an issuer by date".

## Upgrading from earlier versions

v0.1 stored documents with `ObjectId` `_id`s, numbers as `double`, and no `schemaVersion`. The script prints a warning when it finds such documents. Schema version 2 kept the signature fields inside the invoice. In both cases, drop the collections and re-import:

```javascript
use demo
db.cfdi.drop()
db.cfdi_signatures.drop()
```

```bash
python3 converter.py
```

## Known limitations

- Only CFDI 3.3 and later documents (`cfdi:Comprobante` root) are supported. The type and array rules have only been tested with CFDI 3.3 files; see [Roadmap](#roadmap) for CFDI 4.0.
- Attributes that are not in the amount, integer, or date lists are stored as strings. New amount fields from other complements must be added to `DECIMAL_KEYS` in `converter.py`.
- The invoice and its signature document are written separately, not in a transaction.

## Roadmap

Pending work, in suggested order:

1. **CFDI 4.0 and Pagos 2.0 support.** CFDI 4.0 has been the required version since 2023. Its files are accepted, but:
   - New amount fields such as `BaseDR`, `ImporteDR`, `BaseP`, `ImporteP`, and `EquivalenciaDR` are not in `DECIMAL_KEYS` and are stored as strings. Fields starting with `Total` (e.g. `MontoTotalPagos`, `TotalTrasladosBaseIVA16`) are already stored as decimals.
   - Repeatable elements such as `TrasladoDR`, `RetencionDR`, `TrasladoP`, and `RetencionP` are not in `ARRAY_KEYS`, so they are stored as an object when they appear once and as an array when they repeat.
   - Add anonymized CFDI 4.0 samples to `xmls/` to verify the changes.
2. **Automated tests.** Add unit tests (no MongoDB needed) for value conversion, array handling, signature extraction, and `_id` assignment.
3. **Default database name.** The code default and the examples in this README use `demo`, while `.env.example` uses `demo_cfdi`. Align them.
4. **Atlas access.** Use a database user limited to `readWrite` on the target database, and restrict the cluster's IP access list to the machines that run the import.
5. **Secondary indexes.** Create indexes once the main query patterns are known (see [Example queries](#example-queries)).
