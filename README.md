# Dumbdb

It's a dumb database. It is not durable. It does not make any ACID guarantees. There is no rich query language (like SQL).

## Overview

No plain text (SQL like) queries. All querying is done via specific APIs. There
is a separate group of APIs for each type of query.

### Create a table
Create a table with the `create_table` API. Having a primary key column is required.

``` sh
curl localhost:3000/api/v1/ddl/create_table -XPOST -d @create_table.json -H "content-type:application/json" -i
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
curl localhost:3000/api/v1/dml/put_item -XPOST -d @put_item.json -H "content-type:application/json" -i
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
curl localhost:3000/api/v1/dml/get_item -XPOST -d '{"table_name": "authors", "key": "42"}' -H 'content-type:application/json' -i
```

### Filter data

[Future] Filter data via `filter_item` API, using a filter expression. Passing primary key column is not required.

### Update data
[Future] Update data via `update_item` API.

### Supported column types
- `Integer`
- `Float`
- `Text`
- `Boolean`

## Storage

Stores one file per table on disk. Stores it in length-prefixed binary format.
