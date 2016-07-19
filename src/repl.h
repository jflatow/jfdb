#ifndef __REPL_H__
#define __REPL_H__

#include <stdio.h>
#include <inttypes.h>
#include "jfdb.h"
#include "trie.h"

static inline int JFDB_pif_error(JFDB *db, const char *str) {
  if (JFDB_has_error(db))
    fprintf(stderr, "%s: %s\n", str, JFDB_str_error(db));
  return db->error;
}

static inline void print_bits(const uint64_t *data, size_t N) {
  for (size_t b = 0; b < N; b++, data++)
    for (char i = 63; i >= 0; i--)
      printf(i % 8 ? "%"PRIu64"" : "%"PRIu64" ", (*data >> i) & 1);
}

static inline void print_bytes(const uint8_t *data, size_t N) {
  for (size_t i = 0, c = 0; i < N; i++)
    if (data[i] < 32 || data[i] > 126)
      if (c++ % 2)
        printf("\x1b[34m%d\x1b[0m", data[i]);
      else
        printf("\x1b[36m%d\x1b[0m", data[i]);
    else
      printf("%c", data[i]);
}

static inline void print_stem(const JFT_Stem stem) {
  if (stem.pre)
    printf("\x1b[32m%s \x1b[0m", JFT_symbol_name(&stem.pre));
  print_bytes(stem.data, stem.size - (stem.pre ? 1 : 0));
  printf("\n");
}

static inline void print_root(const JFT *node) {
  JFT_Root *root = (JFT_Root *)node;
  printf("root info:\n"
         " primary offset:    %"PRIu64"\n"
         " indices offset:    %"PRIu64"\n"
         " extent:            %"PRIu64"\n"
         " num primary:       %u\n"
         " num indices:       %u\n"
         " max indices:       %u\n"
         " max key size:      %u\n",
         root->primaryOffset,
         root->indicesOffset,
         root->extent,
         root->numPrimary,
         root->numIndices,
         root->maxIndices,
         root->maxKeySize);
}

static inline void print_fork(uint8_t byte, JFT_Offset offset) {
  printf(" ");
  print_bytes(&byte, 1);
  printf(" %"PRIu64"\n", offset);
}

static inline void print_scan(const JFT *node) {
  int length = JFT_type_info(node);
  const uint8_t *symbols = JFT_type_data(node);
  const JFT_Offset *offsets = (JFT_Offset *)(symbols + length);
  printf("scan list:\n");
  for (int i = 0; i < length; i++)
    print_fork(symbols[i], offsets[i]);
}

static inline void print_jump(const JFT *node) {
  int rangeId = JFT_type_info(node);
  JFT_Offset *offsets = (JFT_Offset *)JFT_type_data(node);
  JFT_Range range = JumpTableRanges[rangeId];
  printf("jump table:\n");
  for (int i = 0; i <= range.max - range.min; i++)
    print_fork(range.min + i, offsets[i]);
}

static inline void print_leaf(const JFT *node) {
  JFT_Leaf leaf = JFT_leaf(node);
  printf("leaf size:  %u\n", leaf.size);
  printf("leaf words:");
  for (int i = 0; i < leaf.size && i < 10000; i++) // XXX
    printf(" %"PRIu64"", leaf.data[i]);
  if (leaf.size > 10000) // XXX
    printf(" ...");
  printf("\n");
}

static inline void print_node(const JFT *node) {
  JFT_Stem stem = JFT_stem(node);
  printf("-----\n");
  printf("node:\n"
         " node type:         %s\n"
         " is special:        %u\n"
         " is dirty:          %u\n"
         " stem size:         %u\n"
         " type info:         %u\n"
         " parent offset:     %"PRIu64"\n"
         " stem:              ",
         JFT_node_type_name(node),
         JFT_is_special(node),
         JFT_is_dirty(node),
         stem.size,
         JFT_type_info(node),
         JFT_parent_offset(node));
  print_stem(stem);
  printf("-----\n");
  switch (JFT_node_type(node)) {
    case Root:      print_root(node); break;
    case ScanList:  print_scan(node); break;
    case JumpTable: print_jump(node); break;
    case Leaf:      print_leaf(node); break;
    default: break;
  }
  printf("-----\n");
}

static inline void print_trie(const JFT *node) {
  JFT_Cursor cursor = JFT_cursor((JFT *)node);
  int depth = 0;
  do {
    do {
      for (int i = 0; i < depth; i++)
        printf(" ");
      if (cursor.symbol > 255)
        printf("\x1b[32m%s\x1b[0m\n", JFT_symbol_name(&cursor.symbol));
      else
        printf("\x1b[35m%s\x1b[0m\n", JFT_symbol_name(&cursor.symbol));
    } while (++depth && JFT_cursor_step(&cursor, Forward) < JFT_SYMBOL_NIL);
    while (JFT_cursor_next(&cursor, Forward) >= JFT_SYMBOL_NIL)
      if (!depth-- || JFT_cursor_back(&cursor, Forward) >= JFT_SYMBOL_TOP)
        return;
  } while (depth);
}

#endif /* __REPL_H__ */