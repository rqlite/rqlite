/*
** 2022-01-19
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
** This file implements a virtual-table that returns information about
** how the query planner called the xBestIndex method.  This virtual table
** is intended for testing and debugging only.
**
** The schema of the virtual table is this:
**
**    CREATE TABLE qpvtab(
**      vn     TEXT,           -- Name of an sqlite3_index_info field
**      ix     INTEGER,        -- Array index or value
**      cn     TEXT,           -- Column name
**      op     INTEGER,        -- operator
**      ux     BOOLEAN,        -- "usable" field
**      rhs    TEXT,           -- sqlite3_vtab_rhs_value()
**
**      a, b, c, d, e,         -- Extra columns to attach constraints to
**
**      flags    INTEGER HIDDEN  -- control flags
**    );
**
** The virtual table returns a description of the sqlite3_index_info object
** that was provided to the (successful) xBestIndex method.  There is one
** row in the result table for each field in the sqlite3_index_info object.
**
** The values of the "a" through "e" columns are one of:
**
**    1.   TEXT - the same as the column name
**    2.   INTEGER - 1 for "a", 2 for "b", and so forth
**
** Option 1 is the default behavior.  2 is use if there is a usable
** constraint on "flags" with an integer right-hand side that where the
** value of the right-hand side has its 0x001 bit set.
**
** All constraints on columns "a" through "e" are marked as "omit".
**
** If there is a usable constraint on "flags" that has a RHS value that
** is an integer and that integer has its 0x02 bit set, then the
** orderByConsumed flag is set.
**
** FLAGS SUMMARY:
**
**   0x001               Columns 'a' through 'e' have INT values
**   0x002               orderByConsumed is set
**   0x004               OFFSET and LIMIT have omit set
**
** COMPILE:
**
**   gcc -Wall -g -shared -fPIC -I. qpvtab.c -o qqvtab.so
**
** EXAMPLE USAGE:
**
**   .load ./qpvtab
**   SELECT rowid, *, flags FROM qpvtab(102)
**    WHERE a=19
**      AND b BETWEEN 4.5 and 'hello'
**      AND c<>x'aabbcc'
**    ORDER BY d, e DESC;
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#if !defined(SQLITE_OMIT_VIRTUALTABLE)

/* qpvtab_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct qpvtab_vtab qpvtab_vtab;
struct qpvtab_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
};

/* qpvtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct qpvtab_cursor qpvtab_cursor;
struct qpvtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  const char *zData;         /* Data to return */
  int nData;                 /* Number of bytes of data */
  int flags;                 /* Flags value */
};

/*
** Names of columns
*/
static const char *azColname[] = {
  "vn",
  "ix",
  "cn",
  "op",
  "ux",
  "rhs",
  "a", "b", "c", "d", "e",
  "flags",
  ""
};

/*
** The qpvtabConnect() method is invoked to create a new
** qpvtab virtual table.
*/
static int qpvtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  qpvtab_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
         "CREATE TABLE x("
         " vn TEXT,"
         " ix INT,"
         " cn TEXT,"
         " op INT,"
         " ux BOOLEAN,"
         " rhs TEXT,"
         " a, b, c, d, e,"
         " flags INT HIDDEN)"
       );
#define QPVTAB_VN      0
#define QPVTAB_IX      1
#define QPVTAB_CN      2
#define QPVTAB_OP      3
#define QPVTAB_UX      4
#define QPVTAB_RHS     5
#define QPVTAB_A       6
#define QPVTAB_B       7
#define QPVTAB_C       8
#define QPVTAB_D       9
#define QPVTAB_E      10
#define QPVTAB_FLAGS  11
#define QPVTAB_NONE   12
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for qpvtab_vtab objects.
*/
static int qpvtabDisconnect(sqlite3_vtab *pVtab){
  qpvtab_vtab *p = (qpvtab_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new qpvtab_cursor object.
*/
static int qpvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  qpvtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a qpvtab_cursor.
*/
static int qpvtabClose(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a qpvtab_cursor to its next row of output.
*/
static int qpvtabNext(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  if( pCur->iRowid<pCur->nData ){
    const char *z = &pCur->zData[pCur->iRowid];
    const char *zEnd = strchr(z, '\n');
    if( zEnd ) zEnd++;
    pCur->iRowid = (int)(zEnd - pCur->zData);
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the qpvtab_cursor
** is currently pointing.
*/
static int qpvtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  if( i>=QPVTAB_VN && i<=QPVTAB_RHS && pCur->iRowid<pCur->nData ){
    const char *z = &pCur->zData[pCur->iRowid];
    const char *zEnd;
    int j;
    j = QPVTAB_VN;
    while(1){
      zEnd = strchr(z, j==QPVTAB_RHS ? '\n' : ',');
      if( j==i || zEnd==0 ) break;
      z = zEnd+1;
      j++;
    }
    if( zEnd==z ){
      sqlite3_result_null(ctx);
    }else if( i==QPVTAB_IX || i==QPVTAB_OP || i==QPVTAB_UX ){
      sqlite3_result_int(ctx, atoi(z));
    }else{
      sqlite3_result_text64(ctx, z, zEnd-z, SQLITE_TRANSIENT, SQLITE_UTF8);
    }
  }else if( i>=QPVTAB_A && i<=QPVTAB_E ){
    if( pCur->flags & 0x001 ){
      sqlite3_result_int(ctx, i-QPVTAB_A+1);
    }else{
      char x = 'a'+i-QPVTAB_A;
      sqlite3_result_text64(ctx, &x, 1, SQLITE_TRANSIENT, SQLITE_UTF8);
    }
  }else if( i==QPVTAB_FLAGS ){
    sqlite3_result_int(ctx, pCur->flags);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int qpvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int qpvtabEof(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  return pCur->iRowid>=pCur->nData;
}

/*
** This method is called to "rewind" the qpvtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to qpvtabColumn() or qpvtabRowid() or 
** qpvtabEof().
*/
static int qpvtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  qpvtab_cursor *pCur = (qpvtab_cursor *)pVtabCursor;
  pCur->iRowid = 0;
  pCur->zData = idxStr;
  pCur->nData = (int)strlen(idxStr);
  pCur->flags = idxNum;
  return SQLITE_OK;
}

/*
** Append the text of a value to pStr
*/
static void qpvtabStrAppendValue(
  sqlite3_str *pStr,
  sqlite3_value *pVal
){
  switch( sqlite3_value_type(pVal) ){
    case SQLITE_NULL:
      sqlite3_str_appendf(pStr, "NULL");
      break;
    case SQLITE_INTEGER:
      sqlite3_str_appendf(pStr, "%lld", sqlite3_value_int64(pVal));
      break;
    case SQLITE_FLOAT:
      sqlite3_str_appendf(pStr, "%!f", sqlite3_value_double(pVal));
      break;
    case SQLITE_TEXT: {
      int i;
      const char *a = (const char*)sqlite3_value_text(pVal);
      int n = sqlite3_value_bytes(pVal);
      sqlite3_str_append(pStr, "'", 1);
      for(i=0; i<n; i++){
        char c = a[i];
        if( c=='\n' ) c = ' ';
        sqlite3_str_append(pStr, &c, 1);
        if( c=='\'' ) sqlite3_str_append(pStr, &c, 1);
      }
      sqlite3_str_append(pStr, "'", 1);
      break;
    }
    case SQLITE_BLOB: {
      int i;
      const unsigned char *a = sqlite3_value_blob(pVal);
      int n = sqlite3_value_bytes(pVal);
      sqlite3_str_append(pStr, "x'", 2);
      for(i=0; i<n; i++){
        sqlite3_str_appendf(pStr, "%02x", a[i]);
      }
      sqlite3_str_append(pStr, "'", 1);
      break;
    }
  }
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int qpvtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i, k = 0;
  int rc;
  sqlite3_str_appendf(pStr, "nConstraint,%d,,,,\n", pIdxInfo->nConstraint);
  for(i=0; i<pIdxInfo->nConstraint; i++){
    sqlite3_value *pVal;
    int iCol = pIdxInfo->aConstraint[i].iColumn;
    int op = pIdxInfo->aConstraint[i].op;
    if( iCol==QPVTAB_FLAGS &&  pIdxInfo->aConstraint[i].usable ){
      pVal = 0;
      rc = sqlite3_vtab_rhs_value(pIdxInfo, i, &pVal);
      assert( rc==SQLITE_OK || pVal==0 );
      if( pVal ){
        pIdxInfo->idxNum = sqlite3_value_int(pVal);
        if( pIdxInfo->idxNum & 0x002 ) pIdxInfo->orderByConsumed = 1;
      }
    }
    if( op==SQLITE_INDEX_CONSTRAINT_LIMIT
     || op==SQLITE_INDEX_CONSTRAINT_OFFSET
    ){
      iCol = QPVTAB_NONE;
    }
    sqlite3_str_appendf(pStr,"aConstraint,%d,%s,%d,%d,",
       i,
       azColname[iCol],
       op,
       pIdxInfo->aConstraint[i].usable);
    pVal = 0;
    rc = sqlite3_vtab_rhs_value(pIdxInfo, i, &pVal);
    assert( rc==SQLITE_OK || pVal==0 );
    if( pVal ){
      qpvtabStrAppendValue(pStr, pVal);
    }
    sqlite3_str_append(pStr, "\n", 1);
  }
  for(i=0; i<pIdxInfo->nConstraint; i++){
    int iCol = pIdxInfo->aConstraint[i].iColumn;
    int op = pIdxInfo->aConstraint[i].op;
    if( op==SQLITE_INDEX_CONSTRAINT_LIMIT
     || op==SQLITE_INDEX_CONSTRAINT_OFFSET
    ){
      iCol = QPVTAB_NONE;
    }
    if( iCol>=QPVTAB_A && pIdxInfo->aConstraint[i].usable ){
      pIdxInfo->aConstraintUsage[i].argvIndex = ++k;
      if( iCol<=QPVTAB_FLAGS || (pIdxInfo->idxNum & 0x004)!=0 ){
        pIdxInfo->aConstraintUsage[i].omit = 1;
      }
    }
  }
  sqlite3_str_appendf(pStr, "nOrderBy,%d,,,,\n", pIdxInfo->nOrderBy);
  for(i=0; i<pIdxInfo->nOrderBy; i++){
    int iCol = pIdxInfo->aOrderBy[i].iColumn;
    sqlite3_str_appendf(pStr, "aOrderBy,%d,%s,%d,,\n",i,
      iCol>=0 ? azColname[iCol] : "rowid",
      pIdxInfo->aOrderBy[i].desc
    );
  }
  sqlite3_str_appendf(pStr, "sqlite3_vtab_distinct,%d,,,,\n", 
                      sqlite3_vtab_distinct(pIdxInfo));
  sqlite3_str_appendf(pStr, "idxFlags,%d,,,,\n", pIdxInfo->idxFlags);
  sqlite3_str_appendf(pStr, "colUsed,%d,,,,\n", (int)pIdxInfo->colUsed);
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  sqlite3_str_appendf(pStr, "idxNum,%d,,,,\n", pIdxInfo->idxNum);
  sqlite3_str_appendf(pStr, "orderByConsumed,%d,,,,\n",
                      pIdxInfo->orderByConsumed);
  pIdxInfo->idxStr = sqlite3_str_finish(pStr);
  pIdxInfo->needToFreeIdxStr = 1;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module qpvtabModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ qpvtabConnect,
  /* xBestIndex  */ qpvtabBestIndex,
  /* xDisconnect */ qpvtabDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ qpvtabOpen,
  /* xClose      */ qpvtabClose,
  /* xFilter     */ qpvtabFilter,
  /* xNext       */ qpvtabNext,
  /* xEof        */ qpvtabEof,
  /* xColumn     */ qpvtabColumn,
  /* xRowid      */ qpvtabRowid,
  /* xUpdate     */ 0,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0,
  /* xShadowName */ 0,
  /* xIntegrity  */ 0
};
#endif /* SQLITE_OMIT_VIRTUALTABLE */


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_qpvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "qpvtab", &qpvtabModule, 0);
#endif
  return rc;
}
