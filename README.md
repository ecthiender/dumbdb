# Dumbdb

It's a dumb database. It is not durable. It does not make any ACID guarantees. There is no rich query language (like SQL).

## Overview

No rich text queries (like SQL). All querying is done via specific APIs. There
is a separate API for each kind of query.

### Create a table
Create a table with the `create_table` API. Having a primary key column is required.

``` sh
curl localhost:3000/api/v1/ddl/create_table \
    -XPOST \
    -d @create_table.json \
    -H "content-type:application/json" -i
```

`create_table.json` -

``` json
{
  "name": "authors",
  "columns": [
    {
      "name": "id",
      "type": "Integer"
    },
    {
      "name": "name",
      "type": "Text"
    }
  ],
  "primary_key": "id"
}
```

### Write data
Write data via `put_item` API.

``` sh
curl localhost:3000/api/v1/dml/put_item \
    -XPOST \
    -d @put_item.json \
    -H "content-type:application/json" -i
```

`put_item.json` -

``` json
{
  "table_name": "authors",
  "item": {
    "id": 42,
    "name": "Douglas Adams"
  }
}
```

### Read data
Read data via `get_item` API. Passing the primary key column is required.

``` sh
curl localhost:3000/api/v1/dml/get_item \
    -XPOST \
    -d @get_item.json \
    -H 'content-type:application/json' -i
```

`get_item.json` -

``` json
{
  "table_name": "authors",
  "key": 42
}
```

### Filter data

Filter data via `filter_item` API, using a filter expression. Passing primary key column is not required.

``` sh
curl localhost:3000/api/v1/dml/filter_item \
  -XPOST \
  -d @filter_item.json \
  -H 'content-type:application/json'
```

`filter_item.json` -

``` json
{
  "table_name": "authors",
  "filter": {
    "$or": [
      {
        "column": "id",
        "op": "$eq",
        "value": 42
      },
      {
        "$and": [
          {
            "column": "id",
            "op": "$gt",
            "value": 100
          },
          {
            "column": "id",
            "op": "$lte",
            "value": 1000
          }
        ]
      }
    ]
  }
}
```

### Update data
[Future] Update data via `update_item` API.

### Supported column types
- `Integer`
- `Float`
- `Text`
- `Boolean`

## Storage

Stores one file per table on disk. Stores it in length-prefixed binary format.
