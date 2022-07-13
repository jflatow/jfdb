#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include "../src/jfdb.h"
#include "../src/repl.h"

#define pinfo(fmt, ...) fprintf(stderr, fmt"\n", ##__VA_ARGS__)
#define pwarn(fmt, ...) fprintf(stderr, fmt"\n", ##__VA_ARGS__)

static JFT_Symbol null = 0;

static void print_meta(JFDB *db) {
  JFDB_CheckPoint *cp = &db->tip.cp;
  printf("-----\n");
  printf("magic: %x:%x\n", db->tip.magic, db->tip.version);
  printf("level: %"PRIu64" / %llu\n", cp->levels, 1LLU << db->tip.level);
  printf("-----\n");
  printf("gaps:\n");
  for (int i = 0; i < JFDB_NUM_GAPS; i++)
    printf(" %8u @ %-8u\n", cp->gaps[i].size, cp->gaps[i].block);
  printf("keys: %12zu / %-12"PRIu64"\n", db->kmap.size, cp->lengthKeys);
  printf("vals: %12zu / %-12"PRIu64"\n", db->vmap.size, cp->lengthVals);
  printf("-----\n");
  printf("roots:\n");
  JFT *trie;
  for (JFT_Offset pos = cp->offset; pos; pos = JFT_parent_offset(trie)) {
    trie = JFDB_get_trie(db, pos);
    printf("      %12"PRIu64" = ", pos);
    print_bits((JFT_Head *)trie, 1);
    printf("\n");
  }
  printf("-----\n");
}

static JFT_Status pkey(JFT_Cursor *cursor, JFDB_Slice *slice, JFT_Boolean isTerminal) {
  if (JFT_leaf(cursor->node).size)
    print_stem(isTerminal ? "[val] " : "[sub] ", *slice->stem);
  return Next;
}

static void print_keys(JFDB *db, JFT_Stem *prefix, JFT_Symbol *stop) {
  JFDB_fold(db, prefix, stop, &pkey, NULL, 0);
}

static void print_info(JFDB *db, JFT_Stem *prefix, JFT *node, JFT_Symbol *stop) {
  JFT_Stem key = JFT_key(node, db->keyData), val = (JFT_Stem) {};
  printf("-----\n");
  printf("@ %ld\n", node - db->kmap.map);
  print_stem("> ", *prefix);
  print_stem("< ", key);
  print_node(node);
  if (JFT_node_type(node) == Leaf)
    if (key.pre == JFT_SYMBOL_PRIMARY)
      if (JFDB_get_value(db, node, &val))
        print_stem("= ", val);
  JFT_Cursor cursor = JFT_cursor(node);
  for (JFT_Keys keys = JFT_keys(&cursor, &key, Forward); JFT_keys_next_until(&keys, stop); )
    print_stem(JFT_cursor_at_terminal(&cursor) ? "[val] " : "[sub] ", *keys.stem);
}

static void print_find(JFDB *db, JFT_Stem *prefix, JFT_Symbol *stop) {
  JFT_Cursor cursor;
  if (JFDB_find(db, &cursor, prefix))
    print_info(db, prefix, cursor.node, stop);
}

static void load_input(JFDB *db, FILE *input) {
  int c, s = 0, n = 2;
  JFT_Stem *stems = malloc(n * sizeof(JFT_Stem));
  JFT_Boolean escaped = False;
  char *buf = malloc(1 << 20); // NB: static buffer
  size_t bufSize = 0;
  stems[0] = (JFT_Stem) {
    .size = 0,
    .data = (uint8_t *)buf
  };
  while ((c = getc(input)) != EOF) {
    if (escaped) {
      // copy byte no matter what
      buf[bufSize++] = c;
      stems[s].size++;
      escaped = False;
    } else if (c == '\\') {
      // escape the next byte
      escaped = True;
    } else if (c == '\t') {
      // close out current stem, start next one
      if (++s >= n)
        stems = realloc(stems, (n *= 2) * sizeof(JFT_Stem));
      stems[s] = (JFT_Stem) {
        .size = 0,
        .data = (uint8_t *)&buf[bufSize]
      };
    } else if (c == '\n') {
      // close out current stem, write db, reset
      if (s)
        JFDB_store(db, &stems[0], &stems[1], &stems[2], s > 1 ? s - 1 : 0, 0);
      else
        JFDB_annul(db, &stems[0], 0);
      bufSize = s = 0;
      stems[s].size = 0;
    } else {
      // copy byte
      buf[bufSize++] = c;
      stems[s].size++;
    }
  }
  free(stems);
  free(buf);
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
          " jfdb node /db/path [offset]\n"
          " jfdb keys /db/path [-n] [-p|-i] [prefix]\n"
          " jfdb find /db/path [-n] [-p|-i] [prefix]\n"
          " jfdb load /db/path\n"
          " jfdb wipe /db/path\n"
          " jfdb crush /db/path\n");
  return isError;
}

static JFT_Stem *read_prefix(JFT_Stem *prefix, int omit, int argc, char **argv) {
  JFT_KeySize len, off = 0;
  for (int i = 0; i < argc; i++) {
    len = strlen(argv[i]);
    memcpy(prefix->data + off, argv[i], len);
    prefix->data[off + len] = 0;
    off += len + 1;
  }
  prefix->size += off - (argc && omit); // maybe omit trailing NULL
  return prefix;
}

static int prefix_opts(JFT_Stem *prefix, JFT_Symbol **stop, int argc, char **argv) {
  extern char *optarg;
  extern int optind, optopt;
  int opt, ptr, err = 0, omit = 1;

  static struct option options[] = {
    {"null", no_argument, NULL, 'n'},
    {"stop", no_argument, NULL, 's'},
    {"primary", no_argument, NULL, 'p'},
    {"indices", no_argument, NULL, 'i'},
    {}
  };

  // default
  prefix->pre = JFT_SYMBOL_PRIMARY;
  prefix->size = 1;

  while ((opt = getopt_long(argc, argv, "hnspi", options, &ptr)) != -1) {
    switch (opt) {
      case 'h':
        return usage(0);
      case 'n':
        omit = 0;
        break;
      case 's':
        *stop = &null;
        break;
      case 'p':
        prefix->pre = JFT_SYMBOL_PRIMARY;
        prefix->size = 1;
        break;
      case 'i':
        prefix->pre = JFT_SYMBOL_INDICES;
        prefix->size = 1;
        break;
      case ':':
        pwarn("%s: option requires an argument -- %c", argv[0], optopt);
        err++;
        break;
      case '?':
        pwarn("%s: illegal option -- %c", argv[0], optopt);
        err++;
        break;
    }
  }

  read_prefix(prefix, omit, argc - optind, argv + optind);
  return err;
}

int main(int argc, char **argv) {
  if (pif_error(argc < 2, "No command specified"))
    return usage(-1);
  if (pif_error(argc < 3, "No DB path specified"))
    return usage(-1);

  char *cmd = argv[1];
  char *keyData[JFT_KEY_LIMIT];
  JFDB *db;
  JFT_Stem prefix = (JFT_Stem) {.data = (uint8_t *)keyData};
  JFT_Symbol *stop = NULL;
  JFT_Offset offset;
  switch (cmd[0]) {
    case 'm':
    case 'n':
    case 'k':
    case 'f':
    case 'l':
    case 'c':
      db = JFDB_open(argv[2], 0);
      if (JFDB_pif_error(db, "Failed to open"))
        return -1;
      switch (cmd[0]) {
        case 'm':
          print_meta(db);
          break;
        case 'n':
          offset = argc > 3 ? atoi(argv[3]) : db->tip.cp.offset;
          offset = MAX(offset, sizeof(JFDB_Header));
          print_info(db, &prefix, db->kmap.map + offset, &null);
          break;
        case 'k':
          if (!prefix_opts(&prefix, &stop, argc - 2, argv + 2))
            print_keys(db, &prefix, stop);
          break;
        case 'f':
          if (!prefix_opts(&prefix, &stop, argc - 2, argv + 2))
            print_find(db, &prefix, stop);
          break;
        case 'l':
          load_input(db, stdin);
          break;
        case 'c':
          JFDB_crush(db);
          break;
      }
      if (JFDB_close(db))
        return -1;
      break;
    case 'w':
      if (pif_error(JFDB_wipe(argv[2]), "Failed to wipe"))
        return -1;
      break;
    default:
      usage(1);
      break;
  }
  return 0;
}