Hooks
-----

Sqlite allows registering custom functions. These function enable custom
processing of column data as it is read from the disk.

Many ORMs rely on this feature to provide a uniform api weather using sqlite or
other database (for example: datatime manipulation)

Since rqlite is sqlite compatible non-go based ORMs will not be able to provide
the same API. To make these non-go based ORMs usable with rqlite, this folder
contains a set of hooks for suc ORMs which can be registered by rqlite.

for more info: https://github.com/rqlite/rqlite/pull/523


Currently implemented ORM hooks:
- django
