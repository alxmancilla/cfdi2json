# cfdi2json

Converts Mexican electronic invoices (CFDI XML files) into JSON documents and loads them into MongoDB.

## How it works

`converter.py` performs the following steps:

1. Recursively walks a directory and picks up every `*.xml` file.
2. Parses each file and converts it to JSON using the [xmljson](https://pypi.org/project/xmljson/) GData convention. Only plain numbers (no leading zeros, no exponent, at most 15 integer digits) become numbers; codes and identifiers such as `00001000000402532969` or `044` stay strings.
3. Removes XML namespace prefixes from keys (e.g. `{http://www.sat.gob.mx/cfd/3}Comprobante` becomes `Comprobante`) and renames text nodes from `$t` to `value`.
4. Coerces data types:
   - `Comprobante.Fecha` is converted to a `datetime` (stored as a BSON date). Both `YYYY-MM-DDTHH:MM:SS` and `YYYY-MM-DDTHH:MM:SS.ffffff` are accepted; any other format is kept as a string and a warning is printed.
   - Identifier-like fields (`NoCertificado`, `Folio`, `Serie`, `Rfc`, `Nombre`, `NoIdentificacion`, `ClaveProdServ`, `CuentaPredial.Numero`, `NoCertificadoSAT`, etc.) are converted to strings.
   - Single-element `Pagos` and `DoctoRelacionado` entries are wrapped in arrays.
5. Writes the documents to the configured collection (`demo.cfdi` by default) in batches of 500:
   - Documents with a `TimbreFiscalDigital.UUID` are upserted by that UUID, so running the script again replaces existing invoices instead of duplicating them.
   - Documents without a UUID are inserted.

On startup, the script checks the MongoDB connection and creates a unique index on `Comprobante.Complemento.TimbreFiscalDigital.UUID` (applies only to documents that have a UUID). If the collection already contains duplicate UUIDs, index creation fails with a warning and the import continues.

Files that fail to parse, convert, or write are reported on the console with their file name and skipped.

### Supported complements

Type coercion is applied to the following CFDI complements:

- `TimbreFiscalDigital`
- `Pagos` (`pago10`)
- `CFDIRegistroFiscal`
- `ConsumoDeCombustibles`
- `EstadoDeCuentaCombustible`
- `Nomina` (`nomina12`): codes and identifiers such as `NumEmpleado`, `Clave`, `TipoPercepcion`, `TipoDeduccion`, and `CuentaBancaria` are stored as strings

Other complements (e.g. `Donatarias`) are converted and stored without additional type coercion.

## Requirements

- Python 3
- A running MongoDB instance
- Python packages listed in `requirements.txt` (`pymongo`, `xmljson`)

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

The script prints a summary for each batch (new and replaced documents), the start time, end time, and total elapsed time in seconds. If MongoDB cannot be reached within 5 seconds, it prints `Could not connect to MongoDB` and exits.

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

- Documents without a `TimbreFiscalDigital.UUID` (unstamped CFDIs) are inserted again on every run.
- Collections loaded by earlier versions of the script may contain duplicates and values with lost leading zeros. Drop and reload the collection to get clean data and the unique index.
