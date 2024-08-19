/*
** 2015-08-18, 2023-04-28
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
** This file demonstrates how to create a table-valued-function using
** a virtual table.  This demo implements the generate_series() function
** which gives the same results as the eponymous function in PostgreSQL,
** within the limitation that its arguments are signed 64-bit integers.
**
** Considering its equivalents to generate_series(start,stop,step): A
** value V[n] sequence is produced for integer n ascending from 0 where
**  ( V[n] == start + n * step  &&  sgn(V[n] - stop) * sgn(step) >= 0 )
** for each produced value (independent of production time ordering.)
**
** All parameters must be either integer or convertable to integer.
** The start parameter is required.
** The stop parameter defaults to (1<<32)-1 (aka 4294967295 or 0xffffffff)
** The step parameter defaults to 1 and 0 is treated as 1.
**
** Examples:
**
**      SELECT * FROM generate_series(0,100,5);
**
** The query above returns integers from 0 through 100 counting by steps
** of 5.
**
**      SELECT * FROM generate_series(0,100);
**
** Integers from 0 through 100 with a step size of 1.
**
**      SELECT * FROM generate_series(20) LIMIT 10;
**
** Integers 20 through 29.
**
**      SELECT * FROM generate_series(0,-100,-5);
**
** Integers 0 -5 -10 ... -100.
**
**      SELECT * FROM generate_series(0,-1);
**
** Empty sequence.
**
** HOW IT WORKS
**
** The generate_series "function" is really a virtual table with the
** following schema:
**
**     CREATE TABLE generate_series(
**       value,
**       start HIDDEN,
**       stop HIDDEN,
**       step HIDDEN
**     );
**
** The virtual table also has a rowid, logically equivalent to n+1 where
** "n" is the ascending integer in the aforesaid production definition.
**
** Function arguments in queries against this virtual table are translated
** into equality constraints against successive hidden columns.  In other
** words, the following pairs of queries are equivalent to each other:
**
**    SELECT * FROM generate_series(0,100,5);
**    SELECT * FROM generate_series WHERE start=0 AND stop=100 AND step=5;
**
**    SELECT * FROM generate_series(0,100);
**    SELECT * FROM generate_series WHERE start=0 AND stop=100;
**
**    SELECT * FROM generate_series(20) LIMIT 10;
**    SELECT * FROM generate_series WHERE start=20 LIMIT 10;
**
** The generate_series virtual table implementation leaves the xCreate method
** set to NULL.  This means that it is not possible to do a CREATE VIRTUAL
** TABLE command with "generate_series" as the USING argument.  Instead, there
** is a single generate_series virtual table that is always available without
** having to be created first.
**
** The xBestIndex method looks for equality constraints against the hidden
** start, stop, and step columns, and if present, it uses those constraints
** to bound the sequence of generated values.  If the equality constraints
** are missing, it uses 0 for start, 4294967295 for stop, and 1 for step.
** xBestIndex returns a small cost when both start and stop are available,
** and a very large cost if either start or stop are unavailable.  This
** encourages the query planner to order joins such that the bounds of the
** series are well-defined.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <limits.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Return that member of a generate_series(...) sequence whose 0-based
** index is ix. The 0th member is given by smBase. The sequence members
** progress per ix increment by smStep.
*/
static sqlite3_int64 genSeqMember(
  sqlite3_int64 smBase,
  sqlite3_int64 smStep,
  sqlite3_uint64 ix
){
  static const sqlite3_uint64 mxI64 =
      ((sqlite3_uint64)0x7fffffff)<<32 | 0xffffffff;
  if( ix>=mxI64 ){
    /* Get ix into signed i64 range. */
    ix -= mxI64;
    /* With 2's complement ALU, this next can be 1 step, but is split into
     * 2 for UBSAN's satisfaction (and hypothetical 1's complement ALUs.) */
    smBase += (mxI64/2) * smStep;
    smBase += (mxI64 - mxI64/2) * smStep;
  }
  /* Under UBSAN (or on 1's complement machines), must do this last term
   * in steps to avoid the dreaded (and harmless) signed multiply overlow. */
  if( ix>=2 ){
    sqlite3_int64 ix2 = (sqlite3_int64)ix/2;
    smBase += ix2*smStep;
    ix -= ix2;
  }
  return smBase + ((sqlite3_int64)ix)*smStep;
}

typedef unsigned char u8;

typedef struct SequenceSpec {
  sqlite3_int64 iBase;         /* Starting value ("start") */
  sqlite3_int64 iTerm;         /* Given terminal value ("stop") */
  sqlite3_int64 iStep;         /* Increment ("step") */
  sqlite3_uint64 uSeqIndexMax; /* maximum sequence index (aka "n") */
  sqlite3_uint64 uSeqIndexNow; /* Current index during generation */
  sqlite3_int64 iValueNow;     /* Current value during generation */
  u8 isNotEOF;                 /* Sequence generation not exhausted */
  u8 isReversing;              /* Sequence is being reverse generated */
} SequenceSpec;

/*
** Prepare a SequenceSpec for use in generating an integer series
** given initialized iBase, iTerm and iStep values. Sequence is
** initialized per given isReversing. Other members are computed.
*/
static void setupSequence( SequenceSpec *pss ){
  int bSameSigns;
  pss->uSeqIndexMax = 0;
  pss->isNotEOF = 0;
  bSameSigns = (pss->iBase < 0)==(pss->iTerm < 0);
  if( pss->iTerm < pss->iBase ){
    sqlite3_uint64 nuspan = 0;
    if( bSameSigns ){
      nuspan = (sqlite3_uint64)(pss->iBase - pss->iTerm);
    }else{
      /* Under UBSAN (or on 1's complement machines), must do this in steps.
       * In this clause, iBase>=0 and iTerm<0 . */
      nuspan = 1;
      nuspan += pss->iBase;
      nuspan += -(pss->iTerm+1);
    }
    if( pss->iStep<0 ){
      pss->isNotEOF = 1;
      if( nuspan==ULONG_MAX ){
        pss->uSeqIndexMax = ( pss->iStep>LLONG_MIN )? nuspan/-pss->iStep : 1;
      }else if( pss->iStep>LLONG_MIN ){
        pss->uSeqIndexMax = nuspan/-pss->iStep;
      }
    }
  }else if( pss->iTerm > pss->iBase ){
    sqlite3_uint64 puspan = 0;
    if( bSameSigns ){
      puspan = (sqlite3_uint64)(pss->iTerm - pss->iBase);
    }else{
      /* Under UBSAN (or on 1's complement machines), must do this in steps.
       * In this clause, iTerm>=0 and iBase<0 . */
      puspan = 1;
      puspan += pss->iTerm;
      puspan += -(pss->iBase+1);
    }
    if( pss->iStep>0 ){
      pss->isNotEOF = 1;
      pss->uSeqIndexMax = puspan/pss->iStep;
    }
  }else if( pss->iTerm == pss->iBase ){
      pss->isNotEOF = 1;
      pss->uSeqIndexMax = 0;
  }
  pss->uSeqIndexNow = (pss->isReversing)? pss->uSeqIndexMax : 0;
  pss->iValueNow = (pss->isReversing)
    ? genSeqMember(pss->iBase, pss->iStep, pss->uSeqIndexMax)
    : pss->iBase;
}

/*
** Progress sequence generator to yield next value, if any.
** Leave its state to either yield next value or be at EOF.
** Return whether there is a next value, or 0 at EOF.
*/
static int progressSequence( SequenceSpec *pss ){
  if( !pss->isNotEOF ) return 0;
  if( pss->isReversing ){
    if( pss->uSeqIndexNow > 0 ){
      pss->uSeqIndexNow--;
      pss->iValueNow -= pss->iStep;
    }else{
      pss->isNotEOF = 0;
    }
  }else{
    if( pss->uSeqIndexNow < pss->uSeqIndexMax ){
      pss->uSeqIndexNow++;
      pss->iValueNow += pss->iStep;
    }else{
      pss->isNotEOF = 0;
    }
  }
  return pss->isNotEOF;
}

/* series_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct series_cursor series_cursor;
struct series_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  SequenceSpec ss;           /* (this) Derived class data */
};

/*
** The seriesConnect() method is invoked to create a new
** series_vtab that describes the generate_series virtual table.
**
** Think of this routine as the constructor for series_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the series_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against generate_series will look like.
*/
static int seriesConnect(
  sqlite3 *db,
  void *pUnused,
  int argcUnused, const char *const*argvUnused,
  sqlite3_vtab **ppVtab,
  char **pzErrUnused
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define SERIES_COLUMN_VALUE 0
#define SERIES_COLUMN_START 1
#define SERIES_COLUMN_STOP  2
#define SERIES_COLUMN_STEP  3

  (void)pUnused;
  (void)argcUnused;
  (void)argvUnused;
  (void)pzErrUnused;
  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(value,start hidden,stop hidden,step hidden)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS);
  }
  return rc;
}

/*
** This method is the destructor for series_cursor objects.
*/
static int seriesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new series_cursor object.
*/
static int seriesOpen(sqlite3_vtab *pUnused, sqlite3_vtab_cursor **ppCursor){
  series_cursor *pCur;
  (void)pUnused;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a series_cursor.
*/
static int seriesClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}


/*
** Advance a series_cursor to its next row of output.
*/
static int seriesNext(sqlite3_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  progressSequence( & pCur->ss );
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int seriesColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  series_cursor *pCur = (series_cursor*)cur;
  sqlite3_int64 x = 0;
  switch( i ){
    case SERIES_COLUMN_START:  x = pCur->ss.iBase; break;
    case SERIES_COLUMN_STOP:   x = pCur->ss.iTerm; break;
    case SERIES_COLUMN_STEP:   x = pCur->ss.iStep;   break;
    default:                   x = pCur->ss.iValueNow;  break;
  }
  sqlite3_result_int64(ctx, x);
  return SQLITE_OK;
}

#ifndef LARGEST_UINT64
#define LARGEST_UINT64 (0xffffffff|(((sqlite3_uint64)0xffffffff)<<32))
#endif

/*
** Return the rowid for the current row, logically equivalent to n+1 where
** "n" is the ascending integer in the aforesaid production definition.
*/
static int seriesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  series_cursor *pCur = (series_cursor*)cur;
  sqlite3_uint64 n = pCur->ss.uSeqIndexNow;
  *pRowid = (sqlite3_int64)((n<LARGEST_UINT64)? n+1 : 0);
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int seriesEof(sqlite3_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  return !pCur->ss.isNotEOF;
}

/* True to cause run-time checking of the start=, stop=, and/or step=
** parameters.  The only reason to do this is for testing the
** constraint checking logic for virtual tables in the SQLite core.
*/
#ifndef SQLITE_SERIES_CONSTRAINT_VERIFY
# define SQLITE_SERIES_CONSTRAINT_VERIFY 0
#endif

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to seriesColumn() or seriesRowid() or
** seriesEof().
**
** The query plan selected by seriesBestIndex is passed in the idxNum
** parameter.  (idxStr is not used in this implementation.)  idxNum
** is a bitmask showing which constraints are available:
**
**   0x01:    start=VALUE
**   0x02:    stop=VALUE
**   0x04:    step=VALUE
**   0x08:    descending order
**   0x10:    ascending order
**   0x20:    LIMIT  VALUE
**   0x40:    OFFSET  VALUE
**
** This routine should initialize the cursor and position it so that it
** is pointing at the first row, or pointing off the end of the table
** (so that seriesEof() will return true) if the table is empty.
*/
static int seriesFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStrUnused,
  int argc, sqlite3_value **argv
){
  series_cursor *pCur = (series_cursor *)pVtabCursor;
  int i = 0;
  (void)idxStrUnused;
  if( idxNum & 0x01 ){
    pCur->ss.iBase = sqlite3_value_int64(argv[i++]);
  }else{
    pCur->ss.iBase = 0;
  }
  if( idxNum & 0x02 ){
    pCur->ss.iTerm = sqlite3_value_int64(argv[i++]);
  }else{
    pCur->ss.iTerm = 0xffffffff;
  }
  if( idxNum & 0x04 ){
    pCur->ss.iStep = sqlite3_value_int64(argv[i++]);
    if( pCur->ss.iStep==0 ){
      pCur->ss.iStep = 1;
    }else if( pCur->ss.iStep<0 ){
      if( (idxNum & 0x10)==0 ) idxNum |= 0x08;
    }
  }else{
    pCur->ss.iStep = 1;
  }
  if( idxNum & 0x20 ){
    sqlite3_int64 iLimit = sqlite3_value_int64(argv[i++]);
    sqlite3_int64 iTerm;
    if( idxNum & 0x40 ){
      sqlite3_int64 iOffset = sqlite3_value_int64(argv[i++]);
      if( iOffset>0 ){
        pCur->ss.iBase += pCur->ss.iStep*iOffset;
      }
    }
    if( iLimit>=0 ){
      iTerm = pCur->ss.iBase + (iLimit - 1)*pCur->ss.iStep;
      if( pCur->ss.iStep<0 ){
        if( iTerm>pCur->ss.iTerm ) pCur->ss.iTerm = iTerm;
      }else{
        if( iTerm<pCur->ss.iTerm ) pCur->ss.iTerm = iTerm;
      }
    }
  }
  for(i=0; i<argc; i++){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ){
      /* If any of the constraints have a NULL value, then return no rows.
      ** See ticket https://www.sqlite.org/src/info/fac496b61722daf2 */
      pCur->ss.iBase = 1;
      pCur->ss.iTerm = 0;
      pCur->ss.iStep = 1;
      break;
    }
  }
  if( idxNum & 0x08 ){
    pCur->ss.isReversing = pCur->ss.iStep > 0;
  }else{
    pCur->ss.isReversing = pCur->ss.iStep < 0;
  }
  setupSequence( &pCur->ss );
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the generate_series virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
**
** In this implementation idxNum is used to represent the
** query plan.  idxStr is unused.
**
** The query plan is represented by bits in idxNum:
**
**   0x01  start = $value  -- constraint exists
**   0x02  stop = $value   -- constraint exists
**   0x04  step = $value   -- constraint exists
**   0x08  output is in descending order
**   0x10  output is in ascending order
**   0x20  LIMIT $value    -- constraint exists
**   0x40  OFFSET $value   -- constraint exists
*/
static int seriesBestIndex(
  sqlite3_vtab *pVTab,
  sqlite3_index_info *pIdxInfo
){
  int i, j;              /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
#ifndef ZERO_ARGUMENT_GENERATE_SERIES
  int bStartSeen = 0;    /* EQ constraint seen on the START column */
#endif
  int unusableMask = 0;  /* Mask of unusable constraints */
  int nArg = 0;          /* Number of arguments that seriesFilter() expects */
  int aIdx[5];           /* Constraints on start, stop, step, LIMIT, OFFSET */
  const struct sqlite3_index_constraint *pConstraint;

  /* This implementation assumes that the start, stop, and step columns
  ** are the last three columns in the virtual table. */
  assert( SERIES_COLUMN_STOP == SERIES_COLUMN_START+1 );
  assert( SERIES_COLUMN_STEP == SERIES_COLUMN_START+2 );

  aIdx[0] = aIdx[1] = aIdx[2] = aIdx[3] = aIdx[4] = -1;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    int iCol;    /* 0 for start, 1 for stop, 2 for step */
    int iMask;   /* bitmask for those column */
    int op = pConstraint->op;
    if( op>=SQLITE_INDEX_CONSTRAINT_LIMIT
     && op<=SQLITE_INDEX_CONSTRAINT_OFFSET
    ){
      if( pConstraint->usable==0 ){
        /* do nothing */
      }else if( op==SQLITE_INDEX_CONSTRAINT_LIMIT ){
        aIdx[3] = i;
        idxNum |= 0x20;
      }else{
        assert( op==SQLITE_INDEX_CONSTRAINT_OFFSET );
        aIdx[4] = i;
        idxNum |= 0x40;
      }
      continue;
    }
    if( pConstraint->iColumn<SERIES_COLUMN_START ) continue;
    iCol = pConstraint->iColumn - SERIES_COLUMN_START;
    assert( iCol>=0 && iCol<=2 );
    iMask = 1 << iCol;
#ifndef ZERO_ARGUMENT_GENERATE_SERIES
    if( iCol==0 && op==SQLITE_INDEX_CONSTRAINT_EQ ){
      bStartSeen = 1;
    }
#endif
    if( pConstraint->usable==0 ){
      unusableMask |=  iMask;
      continue;
    }else if( op==SQLITE_INDEX_CONSTRAINT_EQ ){
      idxNum |= iMask;
      aIdx[iCol] = i;
    }
  }
  if( aIdx[3]==0 ){
    /* Ignore OFFSET if LIMIT is omitted */
    idxNum &= ~0x60;
    aIdx[4] = 0;
  }
  for(i=0; i<5; i++){
    if( (j = aIdx[i])>=0 ){
      pIdxInfo->aConstraintUsage[j].argvIndex = ++nArg;
      pIdxInfo->aConstraintUsage[j].omit =
         !SQLITE_SERIES_CONSTRAINT_VERIFY || i>=3;
    }
  }
  /* The current generate_column() implementation requires at least one
  ** argument (the START value).  Legacy versions assumed START=0 if the
  ** first argument was omitted.  Compile with -DZERO_ARGUMENT_GENERATE_SERIES
  ** to obtain the legacy behavior */
#ifndef ZERO_ARGUMENT_GENERATE_SERIES
  if( !bStartSeen ){
    sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = sqlite3_mprintf(
        "first argument to \"generate_series()\" missing or unusable");
    return SQLITE_ERROR;
  }
#endif
  if( (unusableMask & ~idxNum)!=0 ){
    /* The start, stop, and step columns are inputs.  Therefore if there
    ** are unusable constraints on any of start, stop, or step then
    ** this plan is unusable */
    return SQLITE_CONSTRAINT;
  }
  if( (idxNum & 0x03)==0x03 ){
    /* Both start= and stop= boundaries are available.  This is the 
    ** the preferred case */
    pIdxInfo->estimatedCost = (double)(2 - ((idxNum&4)!=0));
    pIdxInfo->estimatedRows = 1000;
    if( pIdxInfo->nOrderBy>=1 && pIdxInfo->aOrderBy[0].iColumn==0 ){
      if( pIdxInfo->aOrderBy[0].desc ){
        idxNum |= 0x08;
      }else{
        idxNum |= 0x10;
      }
      pIdxInfo->orderByConsumed = 1;
    }
  }else if( (idxNum & 0x21)==0x21 ){
    /* We have start= and LIMIT */
    pIdxInfo->estimatedRows = 2500;
  }else{
    /* If either boundary is missing, we have to generate a huge span
    ** of numbers.  Make this case very expensive so that the query
    ** planner will work hard to avoid it. */
    pIdxInfo->estimatedRows = 2147483647;
  }
  pIdxInfo->idxNum = idxNum;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** generate_series virtual table.
*/
static sqlite3_module seriesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  seriesConnect,             /* xConnect */
  seriesBestIndex,           /* xBestIndex */
  seriesDisconnect,          /* xDisconnect */
  0,                         /* xDestroy */
  seriesOpen,                /* xOpen - open a cursor */
  seriesClose,               /* xClose - close a cursor */
  seriesFilter,              /* xFilter - configure scan constraints */
  seriesNext,                /* xNext - advance a cursor */
  seriesEof,                 /* xEof - check for end of scan */
  seriesColumn,              /* xColumn - read data */
  seriesRowid,               /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0,                         /* xShadowName */
  0                          /* xIntegrity */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_series_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( sqlite3_libversion_number()<3008012 && pzErrMsg!=0 ){
    *pzErrMsg = sqlite3_mprintf(
        "generate_series() requires SQLite 3.8.12 or later");
    return SQLITE_ERROR;
  }
  rc = sqlite3_create_module(db, "generate_series", &seriesModule, 0);
#endif
  return rc;
}
