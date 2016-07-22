#ifndef __JFDB_H__
#define __JFDB_H__

#include <stdint.h>

#include "trie.h"

#define JFDB_MAGIC        0x7FDB
#define JFDB_VERSION      0x0000

#define JFDB_KEYS         0b01
#define JFDB_VALS         0b10

#define JFDB_OK           0
#define JFDB_ERROR        1
#define JFDB_ENOMEM       2
#define JFDB_EPATH        3
#define JFDB_EFILE        4
#define JFDB_EMMAP        5
#define JFDB_ETRIE        6

#define JFDB_TEMPORARY    0b0001  // open flag
#define JFDB_FLUSH        0b0001  // write flag

#define JFDB_MAX_FREE     256
#define JFDB_MAX_ERRSTR   128
#define JFDB_MAX_PATH     1024

#define JFDB_BLOCK_SIZE   256
#define JFDB_NUM_GAPS     8

typedef struct {
  uint32_t block;            // number of blocks
  uint32_t size;             // number of bytes
} JFDB_Region;

typedef struct {
  uint32_t begin;
  uint32_t size;
  uint64_t data[];
} JFDB_RegionMap;

typedef struct {
  JFDB_Region gaps[JFDB_NUM_GAPS];
  uint64_t lengthKeys;
  uint64_t lengthVals;
  uint64_t levels;
  uint64_t offset;
} JFDB_CheckPoint;

typedef struct {
  uint32_t magic;
  uint32_t version;
  uint8_t level;
  uint8_t reserved[7];
  JFDB_CheckPoint cp;
} JFDB_Header;

typedef struct {
  int fd;
  uint8_t *map;
  size_t size;
} JFDB_MMap;

typedef struct {
  uint32_t lower;
  uint32_t upper;
  uint64_t size;
  JFDB_Region data[JFDB_MAX_FREE + JFDB_NUM_GAPS];
} JFDB_FreeList;

typedef struct {
  char *path;
  char errstr[JFDB_MAX_ERRSTR];
  int error;
  int flags;
  JFDB_MMap kmap;
  JFDB_MMap vmap;
  JFDB_Header tip;
  JFDB_FreeList freeList;
  JFT_Boolean isModified;
  JFT_Boolean oughtFlush;
  JFT_Buffer *scratch[2];
  uint8_t keyData[JFT_KEY_LIMIT];
} JFDB;

/* Crude API */

int JFDB_wipe(const char *path);

/* Basic API */

JFDB *JFDB_open(const char *path, int flags);
JFDB *JFDB_close(JFDB *db);

JFDB *JFDB_write(JFDB *db, const JFT *tx, int flags);
JFDB *JFDB_annul(JFDB *db, const JFT_Stem *primary, int flags);
JFDB *JFDB_store(JFDB *db,
                 const JFT_Stem *primary,
                 const JFT_Stem *value,
                 const JFT_Stem *indices,
                 JFT_Count numIndices,
                 int flags);

JFDB *JFDB_flush(JFDB *db);
JFDB *JFDB_crush(JFDB *db);
JFDB *JFDB_compact(JFDB *db, const JFT *tx);

JFT *JFDB_get_trie(JFDB *db, JFT_Offset offset);
JFT_Stem *JFDB_get_value(JFDB *db, JFT *node, JFT_Stem *value);
JFT_Cursor *JFDB_find(JFDB *db, JFT_Cursor *cursor, const JFT_Stem *key);

/* Error handling */

static inline int JFDB_has_error(JFDB *db) {
  return db == NULL || db->error;
}

static inline const char *JFDB_str_error(JFDB *db) {
  if (db == NULL)
    return "db not allocated";
  return db->errstr;
}

/* Folding & Iterating
 *
 * Fold happens simultanously across all levels of the DB.
 * Its the only way to traverse in key order when there are multiple levels.
 * Its cheaper to iterate and operate on each JFT independently when possible.
 */


typedef struct JFDB_Slice JFDB_Slice;
typedef JFT_Status (*JFDB_FoldFun)(JFT_Cursor *cursor, JFDB_Slice *slice, JFT_Boolean isTerminal);

struct JFDB_Slice {
  JFDB *db;
  JFT_Stem *stem;
  JFT_Symbol *stop;
  JFT_KeySize zero;
  JFT_KeySize nth;
  JFDB_FoldFun fun;
  void *acc;
};

JFT_Status JFDB_fold(JFDB *db, JFT_Stem *stem, JFT_Symbol *stop, JFDB_FoldFun fun, void *acc, int flags);

#endif /* __JFDB_H__ */
