#include <stdio.h>
#include <stdlib.h>
#include "../src/jfdb.h"
#include "../src/repl.h"

static void print_meta(JFDB *db) {
  JFDB_CheckPoint *cp = &db->tip.cp;
  printf("-----\n");
  printf("magic: %x:%x\n", db->tip.magic, db->tip.version);
  printf("level: %llu / %llu\n", cp->levels, 1LLU << db->tip.level);
  printf("-----\n");
  printf("gaps:\n");
  for (int i = 0; i < JFDB_NUM_GAPS; i++)
    printf(" %8u @ %-8u\n", cp->gaps[i].size, cp->gaps[i].block);
  printf("keys: %12zu / %-12llu\n", db->kmap.size, cp->lengthKeys);
  printf("vals: %12zu / %-12llu\n", db->vmap.size, cp->lengthVals);
  printf("-----\n");
  printf("roots:\n");
  JFT *trie;
  for (JFT_Offset pos = cp->offset; pos; pos = JFT_parent_offset(trie)) {
    trie = JFDB_get_trie(db, pos);
    printf("      %12llu = ", pos);
    print_bits((JFT_Head *)trie, 1);
    printf("\n");
  }
  printf("-----\n");
}

static JFT_Status pkey(JFT_Cursor *cursor, JFDB_Slice *slice, JFT_Boolean isTerminal) {
  printf(isTerminal ? "[val] " : "[sub] ");
  print_stem(*slice->stem);
  return Next;
}

static void print_keys(JFDB *db, JFT_Symbol *stop) {
  JFT_Stem stem = (JFT_Stem) {.data = db->keyData};
  JFDB_fold(db, &stem, stop, &pkey, NULL, 0);
}

static void print_info(JFDB *db, JFT *node, JFT_Symbol *stop) {
  JFT_Stem key = (JFT_Stem) {.data = db->keyData};
  JFT_Keys keys;
  JFT_Cursor cursor = JFT_cursor(node);
  printf("-----\n@ %ld ~ ", node - db->kmap.map);
  print_stem(JFT_key(cursor.node, db->keyData));
  print_node(cursor.node);
  for (keys = JFT_keys(&cursor, &key, Forward); JFT_keys_next_until(&keys, stop); ) {
    if (JFT_cursor_at_terminal(&cursor))
      printf("[val] ");
    else
      printf("[sub] ");
    print_stem(*keys.stem);
  }
}

static void print_match(JFDB *db, JFT_Stem *prefix, JFT_Symbol *stop, int N, char **argv) {
  JFT_Cursor cursor;
  JFT_KeySize len, off = 0;
  for (int i = 0; i < N; i++) {
    len = strlen(argv[i]);
    memcpy(prefix->data + off, argv[i], len);
    prefix->data[off + len] = 0;
    off += len + 1;
  }
  prefix->size += off;
  JFDB_find(db, &cursor, prefix);
  print_info(db, cursor.node, stop);
}

static int pif_error(int test, const char *str) {
  if (test)
    fprintf(stderr, "ERROR: %s\n", str);
  return test;
}

static int usage(int isError) {
  fprintf(isError ? stderr : stdout,
          "Usage: \n"
          " jfdb meta /db/path\n"
          " jfdb fold /db/path [stop?]\n"
          " jfdb node /db/path [offset]\n"
          " jfdb primary /db/path [prefix]\n"
          " jfdb indices /db/path [prefix]\n");
  return isError;
}

int main(int argc, char **argv) {
  if (pif_error(argc < 2, "No command specified"))
    return usage(-1);
  if (pif_error(argc < 3, "No DB path specified"))
    return usage(-1);

  char *cmd = argv[1];
  JFDB *db = JFDB_open(argv[2], 0);
  if (JFDB_pif_error(db, "Failed to open"))
    return -1;

  char *keyData[JFT_KEY_LIMIT];
  JFT_Stem prefix = (JFT_Stem) {.data = (uint8_t *)keyData};
  JFT_Symbol null = 0;
  JFT_Offset offset;
  switch (cmd[0]) {
    case 'm':
      print_meta(db);
      break;
    case 'f':
      print_keys(db, argc > 3 ? &null : NULL);
      break;
    case 'n':
      offset = argc > 3 ? atoi(argv[3]) : db->tip.cp.offset;
      offset = MAX(offset, sizeof(JFDB_Header));
      print_info(db, db->kmap.map + offset, &null);
      break;
    case 'p':
      prefix.pre = JFT_SYMBOL_PRIMARY;
      prefix.size = 1;
      print_match(db, &prefix, &null, argc - 3, argv + 3);
      break;
    case 'i':
      prefix.pre = JFT_SYMBOL_INDICES;
      prefix.size = 1;
      print_match(db, &prefix, &null, argc - 3, argv + 3);
      break;
    default:
      usage(1);
      break;
  }

  JFDB_close(db);
  return 0;
}