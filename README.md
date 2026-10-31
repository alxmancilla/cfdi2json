# cfdi2json

Converts Mexican electronic invoices (CFDI XML files) into JSON documents and loads them into MongoDB.

## How it works

`converter.py` performs the following steps:

1. Recursively walks a directory and picks up every `*.xml` file.
2. Parses each file and converts it to JSON using the [xmljson](https://pypi.org/project/xmljson/) GData convention.
3. Removes XML namespace prefixes from keys (e.g. `{http://www.sat.gob.mx/cfd/3}Comprobante` becomes `Comprobante`) and renames text nodes from `$t` to `value`.
4. Coerces data types:
   - `Comprobante.Fecha` is converted to a `datetime` (stored as a BSON date).
   - Identifier-like fields (`NoCertificado`, `Folio`, `Serie`, `Rfc`, `Nombre`, `NoIdentificacion`, `ClaveProdServ`, `CuentaPredial.Numero`, `NoCertificadoSAT`, etc.) are converted to strings.
   - Single-element `Pagos` and `DoctoRelacionado` entries are wrapped in arrays.
5. Inserts each document into the configured collection (`demo.cfdi` by default).

Files that fail to parse, convert, or insert are reported on the console and skipped.

### Supported complements

Type coercion is applied to the following CFDI complements:

- `TimbreFiscalDigital`
- `Pagos` (`pago10`)
- `CFDIRegistroFiscal`
- `ConsumoDeCombustibles`
- `EstadoDeCuentaCombustible`

Other complements (e.g. `Nomina`, `Donatarias`) are converted and stored without additional type coercion.

## Requirements

- Python 3
- A running MongoDB instance
- Python packages: `xmljson`, `pymongo`

```bash
pip install xmljson pymongo
```

## Configuration

Settings are read from a `.env` file in the project directory. Environment variables already set in the shell take precedence over values in `.env`.

| Variable             | `.env` value                 | Description                                  |
|----------------------|------------------------------|----------------------------------------------|
| `MONGODB_URI`        | `mongodb://localhost:27017`  | MongoDB connection string                    |
| `MONGODB_DATABASE`   | `demo`                       | Target database                              |
| `MONGODB_COLLECTION` | `cfdi`                       | Target collection                            |
| `DATADIR`            | `./xmls`                     | Directory scanned when no argument is given  |

If a variable is not set, `converter.py` uses the same defaults, except `DATADIR`, which falls back to `/home/ec2-user/xmls`. A relative `DATADIR` is resolved against the current working directory.

Example override from the shell:

```bash
MONGODB_URI="mongodb+srv://<user>:<password>@<cluster>/" python3 converter.py
```

> Do not commit real credentials in `.env`.

## Usage

```bash
python3 converter.py [path/to/xml/directory]
```

With the default `.env`, running the script without arguments loads the bundled sample files from `./xmls`.

The script prints the start time, end time, and total elapsed time.

## Sample data

The `xmls/` directory contains 24 sample CFDI 3.3 invoices, including payment (`pago10`), payroll (`nomina12`), and donation (`donat`) complements.

## Example query

```javascript
use demo
db.cfdi.find(
  { "Comprobante.Fecha": { $gte: ISODate("2018-01-01") } },
  { "Comprobante.Emisor.Rfc": 1, "Comprobante.Total": 1 }
)
```

## Known limitations

- Running the script more than once on the same files inserts duplicate documents (no unique index or upsert on the `TimbreFiscalDigital.UUID`).
- xmljson converts numeric-looking attribute values to numbers by default, so leading zeros in fields such as `NoCertificado` are lost before they are converted back to strings.
- `Fecha` values must use the `YYYY-MM-DDTHH:MM:SS` format; files with any other format are skipped.
- Documents are inserted one at a time.
