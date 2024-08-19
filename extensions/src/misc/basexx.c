/*
** 2022-11-20
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
** This source allows multiple SQLite extensions to be either: combined
** into a single runtime-loadable library; or built into the SQLite shell
** using a preprocessing convention set by src/shell.c.in (and shell.c).
**
** Presently, it combines the base64.c and base85.c extensions. However,
** it can be used as a template for other combinations.
**
** Example usages:
**
**  - Build a runtime-loadable extension from SQLite checkout directory:
** *Nix, OSX: gcc -O2 -shared -I. -fPIC -o basexx.so ext/misc/basexx.c
** Win32: cl /Os -I. ext/misc/basexx.c -link -dll -out:basexx.dll
**
**  - Incorporate as built-in in sqlite3 shell:
** *Nix, OSX with gcc on a like platform:
**  export mop1=-DSQLITE_SHELL_EXTSRC=ext/misc/basexx.c
**  export mop2=-DSQLITE_SHELL_EXTFUNCS=BASEXX
**  make sqlite3 "OPTS=$mop1 $mop2"
** Win32 with Microsoft toolset on Windows:
**  set mop1=-DSQLITE_SHELL_EXTSRC=ext/misc/basexx.c
**  set mop2=-DSQLITE_SHELL_EXTFUNCS=BASEXX
**  set mops="OPTS=%mop1% %mop2%"
**  nmake -f Makefile.msc sqlite3.exe %mops%
*/

#ifndef SQLITE_SHELL_EXTFUNCS /* Guard for #include as built-in extension. */
# include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1;
#endif

static void init_api_ptr(const sqlite3_api_routines *pApi){
  SQLITE_EXTENSION_INIT2(pApi);
}

#undef SQLITE_EXTENSION_INIT1
#define SQLITE_EXTENSION_INIT1 /* */
#undef SQLITE_EXTENSION_INIT2
#define SQLITE_EXTENSION_INIT2(v) (void)v

typedef unsigned char u8;
#define U8_TYPEDEF

/* These next 2 undef's are only needed because the entry point names
 * collide when formulated per the rules stated for loadable extension
 * entry point names that will be deduced from the file basenames.
 */
#undef sqlite3_base_init
#define sqlite3_base_init sqlite3_base64_init
#include "base64.c"

#undef sqlite3_base_init
#define sqlite3_base_init sqlite3_base85_init
#include "base85.c"

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_basexx_init(sqlite3 *db, char **pzErr,
                               const sqlite3_api_routines *pApi){
  int rc1;
  int rc2;

  init_api_ptr(pApi);
  rc1 = BASE64_INIT(db);
  rc2 = BASE85_INIT(db);

  if( rc1==SQLITE_OK && rc2==SQLITE_OK ){
    BASE64_EXPOSE(db, pzErr);
    BASE64_EXPOSE(db, pzErr);
    return SQLITE_OK;
  }else{
    return SQLITE_ERROR;
  }
}

# define BASEXX_INIT(db) sqlite3_basexx_init(db, 0, 0)
# define BASEXX_EXPOSE(db, pzErr) /* Not needed, ..._init() does this. */
