# PRAGMA Directives
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

You can issue [`PRAGMA`](https://www.sqlite.org/pragma.html) directives to rqlite, and they will be passed to the underlying SQLite database. Certain `PRAGMA` directives, which alter the operation of the SQLite database, may not make sense in the context of rqlite (since rqlite does not given direct control over its connections to the SQLite database). Furthermore some `PRAGMA` directives may even break rqlite. If you have questions about a specific `PRAGMA` directive, the [rqlite Google Group](https://groups.google.com/group/rqlite) is a good place to start the discussion.

`PRAGMA` directives which just return information about the SQLite database, without changing its operation, are always safe.

## Issuing a `PRAGMA` directive
The rqlite CLI supports issuing `PRAGMA` directives. For example:
```
127.0.0.1:4001> pragma compile_options
+----------------------------+
| compile_options            |
+----------------------------+
| COMPILER=gcc-7.5.0         |
+----------------------------+
| DEFAULT_WAL_SYNCHRONOUS=1  |
+----------------------------+
| ENABLE_DBSTAT_VTAB         |
+----------------------------+
| ENABLE_FTS3                |
+----------------------------+
| ENABLE_FTS3_PARENTHESIS    |
+----------------------------+
| ENABLE_JSON1               |
+----------------------------+
| ENABLE_RTREE               |
+----------------------------+
| ENABLE_UPDATE_DELETE_LIMIT |
+----------------------------+
| OMIT_DEPRECATED            |
+----------------------------+
| OMIT_SHARED_CACHE          |
+----------------------------+
| SYSTEM_MALLOC              |
+----------------------------+
| THREADSAFE=1               |
+----------------------------+
```

`PRAGMA` directives may also be issued using the `/db/execute` or `/db/query` endpoint. For example:
```
$ curl -G 'localhost:4001/db/query?pretty&timings' --data-urlencode 'q=PRAGMA foreign_keys'                                                                        
{                                                                                                                                                                                                                        
    "results": [                                                                                                                                                                                                         
        {                                                                                                                                                                                                                
            "columns": [                                                                                                                                                                                                 
                "foreign_keys"                                                                                                                                                                                           
            ],                                                                                                                                                                                                           
            "types": [                                                                                                                                                                                                   
                ""                                                                                                                                                                                                       
            ],                                                                                                                                                                                                           
            "values": [                                                                                                                                                                                                  
                [                                                                                                                                                                                                        
                    0                                                                                                                                                                                                    
                ]
            ],
            "time": 0.000070499
        }
    ],
    "time": 0.000540857
}$
```
