#ifndef __JFDB_DB_H__
#define __JFDB_DB_H__

#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include "jfdb.h"

/* Internal shared definitions  */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define RUP(x, y) (((x) + (y) - 1) / (y))
#define RTM(x, m) (RUP(x, m) * (m))

#if _POSIX_SYNCHRONIZED_IO > 0
#define JFDB_fsync fdatasync
#else
#define JFDB_fsync fsync
#endif

#define JFDB_msync msync

static inline JFDB *JFDB_set_error(JFDB *db, int code, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  db->error = code;
  vsnprintf(db->errstr, JFDB_MAX_ERRSTR, fmt, ap);
  va_end(ap);
  return db;
}

static inline JFDB_RegionMap *JFDB_region_map_alloc(uint32_t lower, uint32_t upper, int invert) {
  // NB: lower <= S < upper
  size_t numBytes = RUP(upper - lower, 8);
  size_t numWords = RUP(numBytes, sizeof(uint64_t));
  JFDB_RegionMap *map = malloc(sizeof(JFDB_RegionMap) + numWords * sizeof(uint64_t));
  if (map) {
    map->begin = (lower / 64) * 64;
    map->size = numWords;
    memset(map->data, invert ? 0xFF : 0, numWords * sizeof(uint64_t));
    if (lower % 64)
      map->data[0] |= -1LLU >> (64 - (lower % 64));
    if (upper % 64)
      map->data[numWords - 1] |= -1LLU << (upper % 64);
  }
  return map;
}

static inline void JFDB_region_map_store(JFDB_RegionMap *map, JFDB_Region region) {
  // set numSlots bits from start
  //  * region.block must be in map range
  // otherwise behavior is undefined
  if (region.size) {
    size_t numSlots = RUP(region.size, JFDB_BLOCK_SIZE);
    size_t start = region.block - map->begin, stop = start + numSlots - 1;
    size_t startWord = start / 64, stopWord = stop / 64;
    if (startWord == stopWord) {
      map->data[startWord] |=
        (-1LLU << (start % 64)) &
        (-1LLU >> (63 - (stop % 64)));
    } else {
      map->data[startWord] |= -1LLU << (start % 64);
      while (++startWord < stopWord)
        map->data[startWord] = -1LLU;
      map->data[stopWord] |= -1LLU >> (63 - (stop % 64));
    }
  }
}

static inline void JFDB_region_map_clear(JFDB_RegionMap *map, JFDB_Region region) {
  // clear numSlots bits from start
  // same caveats as with store
  if (region.size) {
    size_t numSlots = RUP(region.size, JFDB_BLOCK_SIZE);
    size_t start = region.block - map->begin, stop = start + numSlots - 1;
    size_t startWord = start / 64, stopWord = stop / 64;
    if (startWord == stopWord) {
      map->data[startWord] &=
        (-1LLU >> (64 - (start % 64))) |
        (-1LLU << ((stop % 64) + 1));
    } else {
      map->data[startWord] &= -1LLU >> (64 - (start % 64));
      while (++startWord < stopWord)
        map->data[startWord] = 0;
      map->data[stopWord] &= -1LLU << ((stop % 64) + 1);
    }
  }
}

static inline void JFDB_region_map_gaps(JFDB_RegionMap *map, JFDB_Region *regions) {
  // initialize the counters and the min, clear the regions
  uint32_t block = 0, count = 0, size;
  JFDB_Region *minMax = &regions[0];
  memset(regions, 0, JFDB_NUM_GAPS * sizeof(JFDB_Region));
  for (size_t i = 0; i < map->size; i++) {
    uint64_t word = map->data[i];
    if (word == 0) {
      count += 64;
    } else if (word == -1) {
      // skip bits
    } else {
      for (int b = ffsll(word), pos = b; pos < 64; pos += (b = ffsll(word >> pos))) {
        if (b) {
          // found the next bit, add leading zeroes (end region)
          // position is right after the bit
          count += b - 1;
          // possibly find / replace min
          if ((size = count * JFDB_BLOCK_SIZE) > minMax->size) {
            minMax->size = size;
            minMax->block = block + map->begin;
            minMax = &regions[0];
            for (int j = 1; j < JFDB_NUM_GAPS; j++)
              if (regions[j].size < minMax->size)
                minMax = &regions[j];
          }
          // find the next zero bit, reset the count (begin region)
          // guaranteed to be a zero since we shifted at least one in
          // position right before the zero
          pos += ffsll(~(word >> pos)) - 1;
          block = 64 * i + pos;
          count = 0;
        } else {
          // add the trailing zeroes to the count
          count += 64 - pos;
          break;
        }
      }
    }
  }
  // possibly find / replace the min with the trailing region
  if ((size = count * JFDB_BLOCK_SIZE) > minMax->size) {
    minMax->size = size;
    minMax->block = block + map->begin;
  }
}

static inline void JFDB_region_map_free(JFDB_RegionMap *map) {
  free(map);
}

#endif /* __JFDB_DB_H__ */