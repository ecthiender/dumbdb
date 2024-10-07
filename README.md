# Dumbdb

It's a dumb database.

## Storage

Stores one file per table on disk. Stores it in binary format.

## Flow

No plain text (SQL like) queries. All querying is done via specific APIs. There
is a separate group of APIs for each type of query.

1. Create a table with a `create_table` API. Having an id (unique identifier) column is required.
2. Write data via `put_item` API.
3. Read data via `get_item` API. Passing the id column is required. 
4. [Future] Filter data via `filter_item` API. (Passing id column is not required)
5. [Future] Update data via `update_item` API.
