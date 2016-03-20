#include <stdio.h>
#include <string.h>
#include "../src/db.h"
#include "../src/repl.h"

static int test_region_map_pos() {
  JFDB_Region gaps[JFDB_NUM_GAPS];
  JFDB_RegionMap *map = JFDB_region_map_alloc(0, 256, 0);
  JFDB_region_map_store(map, (JFDB_Region) {
      .block = 3,
      .size = 512
  });
  JFDB_region_map_store(map, (JFDB_Region) {
      .block = 10,
      .size = 512
  });
  JFDB_region_map_store(map, (JFDB_Region) {
      .block = 60,
      .size = 10000
  });
  JFDB_region_map_clear(map, (JFDB_Region) {
      .block = 11,
      .size = 256,
  });
  JFDB_region_map_gaps(map, gaps);
  JFDB_region_map_free(map);

  for (int i = 0; i < JFDB_NUM_GAPS; i++)
    printf("GAP %u %u\n", gaps[i].block, gaps[i].size);
  return 0;
}

static int test_region_map_neg() {
  JFDB_Region gaps[JFDB_NUM_GAPS];
  JFDB_RegionMap *map = JFDB_region_map_alloc(0, 256, 1);
  JFDB_region_map_clear(map, (JFDB_Region) {
      .block = 3,
      .size = 512
  });
  JFDB_region_map_clear(map, (JFDB_Region) {
      .block = 10,
      .size = 512
  });
  JFDB_region_map_clear(map, (JFDB_Region) {
      .block = 60,
      .size = 10000
  });
  JFDB_region_map_store(map, (JFDB_Region) {
      .block = 11,
      .size = 256,
  });
  JFDB_region_map_gaps(map, gaps);
  JFDB_region_map_free(map);

  for (int i = 0; i < JFDB_NUM_GAPS; i++)
    printf("GAP %u %u\n", gaps[i].block, gaps[i].size);
  return 0;
}

static void store_basic(JFDB *db, int flags) {
  const JFT_Stem w[] = {
    JFTK("k1"),
    JFTK("k2"),
    JFTK("value"),
    JFTK("other"),
    JFTK("these"),
    JFTK("are"),
    JFTK("just"),
    JFTK("some"),
    JFTK("strings, maybe a little bit longer this time although not long enough")
  };

  JFDB_store(db, w + 0, w + 2, NULL, 0, flags);  // k1
  JFDB_store(db, w + 1, w + 3, NULL, 0, flags);  // k2
  JFDB_store(db, w + 0, w + 3, NULL, 0, flags);  // k1
  JFDB_store(db, w + 1, w + 2, NULL, 0, flags);  // k2
  JFDB_store(db, w + 4, w + 8, NULL, 0, flags);  // these
  JFDB_store(db, w + 6, w + 7, NULL, 0, flags);  // just
  JFDB_store(db, w + 5, w + 6, w + 2, 6, flags); // are
}

static void store_small(JFDB *db, int flags) {
  for (uint64_t i = 0; i < 10000; i++) {
    JFT_Stem k = (JFT_Stem) {
      .size = 8,
      .data = (uint8_t *)&i
    };
    JFDB_store(db, &k, &k, NULL, 0, flags);
  }
}

static int test_store(JFDB *db, int flags) {
  store_basic(db, flags);
  store_small(db, flags);
  return JFDB_pif_error(db, "Failed to store");
}

static int test_crush(JFDB *db) {
  JFDB_crush(db);
  return JFDB_pif_error(db, "Failed to crush");
}

static int test_db(int argc, char **argv) {
  JFDB *db = JFDB_open(argc > 1 ? argv[1] : "", 0);
  int flags = argc > 2 ? atoi(argv[2]) : 0;

  if (JFDB_pif_error(db, "Failed to open"))
    return -1;

  test_store(db, flags);
  test_crush(db);

  JFDB_close(db);
  return 0;
}
int main(int argc, char **argv) {
  test_region_map_pos();
  test_region_map_neg();
  test_db(argc, argv);
  return 0;
}
