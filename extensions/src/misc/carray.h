/*
** 2020-11-17
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Interface definitions for the CARRAY table-valued function
** extension.
*/

#ifndef _CARRAY_H
#define _CARRAY_H

#include "sqlite3.h"              /* Required for error code definitions */

#ifdef __cplusplus
extern "C" {
#endif

/* Use this interface to bind an array to the single-argument version
** of CARRAY().
*/
SQLITE_API int sqlite3_carray_bind(
  sqlite3_stmt *pStmt,        /* Statement to be bound */
  int i,                      /* Parameter index */
  void *aData,                /* Pointer to array data */
  int nData,                  /* Number of data elements */
  int mFlags,                 /* CARRAY flags */
  void (*xDel)(void*)         /* Destructgor for aData*/
);

/* Allowed values for the mFlags parameter to sqlite3_carray_bind().
*/
#define CARRAY_INT32     0    /* Data is 32-bit signed integers */
#define CARRAY_INT64     1    /* Data is 64-bit signed integers */
#define CARRAY_DOUBLE    2    /* Data is doubles */
#define CARRAY_TEXT      3    /* Data is char* */
#define CARRAY_BLOB      4    /* Data is struct iovec */

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef _CARRAY_H */
