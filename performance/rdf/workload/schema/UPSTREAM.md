# Where `AYA_cancer_schema.jsonld` comes from

The STRONG AYA semantic schema, copied unchanged from
<https://github.com/STRONGAYA/AYA-cancer-semantic-map>:

| | |
|---|---|
| Release | v2.0.1 |
| Commit | `c23c8143d4a7524b8bce34a7bc36562aaad8028f` |
| File | `AYA_cancer_schema.jsonld` |
| SHA-256 | `20b707624a2695044b6f1016ceafeda61d2d5ae47e75947a6241cbffc3dfdca1` |
| Licence | Apache-2.0, as Flyover's |

It is the schema only: 208 variables with their classes, reconstructions and
value mappings, and no centre's database. The benchmark generates the tables
and their local mapping itself (`../generator.py`).

To move to a newer release, replace the file, update this table, and render
the read queries again (`../../queries/render.py`), since they are filled in
from this schema. The pinned bytes in `performance/tests/test_generator.py`
will change with it: results measured before and after are then not on the
same data.
