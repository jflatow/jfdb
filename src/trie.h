#ifndef __JFT_H__
#define __JFT_H__

#include <stdint.h>
#include <string.h>

#define JFT_FLAGS_REVERSE      0b0001
#define JFT_FLAGS_PRUNE        0b0010
#define JFT_FLAGS_ATOM         0b0100

#define JFT_ITER_WORDS         16
#define JFT_KEY_LIMIT	      (1 << 10)    // changeable: max is (1 << 12)
#define JFT_MASK_CAPACITY     (sizeof(JFT_Mask) * 8)
#define JFT_MASK_ACTIVE(N)    (-1LLU >> (JFT_MASK_CAPACITY - (N)))

#define JFT_SYMBOL_ANY        ((JFT_Symbol)-1)
#define JFT_SYMBOL_NIL        ((JFT_Symbol)-2)
#define JFT_SYMBOL_TOP        ((JFT_Symbol)-3)
#define JFT_SYMBOL_INDICES    ((JFT_Symbol)-4)
#define JFT_SYMBOL_PRIMARY    ((JFT_Symbol)-5)

#define JFT_SpToB(S)             ((uint8_t)(S))
#define JFT_BToSp(B)         (((JFT_Symbol)(B)) | 0xFFFFFFFFFFFFFF00)

#define MIN(a, b)            ((a) < (b) ? (a) : (b))
#define MAX(a, b)            ((a) > (b) ? (a) : (b))

#ifdef __GNUC__
#define ffsll(x)             (__builtin_ffsll(x))
#define flsll(x)             ((x) == 0 ? 0 : sizeof(x) * 8 - __builtin_clz(x))
#endif

/* Convenience */

#define JFTK(key) ((JFT_Stem) {.size = strlen(key) + 1, .data = (uint8_t *)key})
#define JFTV(val) ((JFT_Leaf) {.size = 1, .data = (JFT_Word []) {val}})

/* Canonical types */

typedef uint8_t       JFT;                // 1st byte of node
typedef uint64_t      JFT_Head;           // the node head
typedef uint64_t      JFT_Word;           // leaf data unit
typedef uint64_t      JFT_Mask;           // 1 bit per element (max 64)
typedef uint_fast16_t JFT_Amount;         // num things (enough for mask)
typedef uint32_t      JFT_Count;          // num items
typedef uint16_t      JFT_KeySize;        // num symbols (12 bit us)
typedef uint64_t      JFT_Offset;         // num bytes (40 bit)
typedef intmax_t      JFT_StemPos;        // considered infinite
typedef uint_fast16_t JFT_Symbol;         // user gets 8 bits
typedef uint64_t      JFT_SymbolSet[4];   // bitset for user symbols
typedef uint32_t      JFT_TypeInfo;       // per-type info (8 bit)

typedef struct {
  JFT_Symbol min;
  JFT_Symbol max;
} JFT_Range;

typedef enum {
  False,
  True
} JFT_Boolean;

typedef enum {
  Forward   = +1,
  Reverse   = -1
} JFT_Direction;

typedef enum {
  In        = +1,
  Out       = -1
} JFT_Phase;

typedef enum {
  Root,
  ScanList,
  JumpTable,
  Leaf
} JFT_NodeType;

typedef enum {
  Ok,
  Step,
  Next,
  ENumCursors,
  ENoMem,
  EUnknown
} JFT_Status;

typedef struct {
  JFT_Head head;
  JFT_Offset primaryOffset;
  JFT_Offset indicesOffset;
  JFT_Offset extent;
  JFT_Count numPrimary;
  JFT_Count numIndices;
  JFT_Count maxIndices;
  JFT_KeySize maxKeySize;
} JFT_Root;

typedef struct {
  JFT_Symbol pre;            // any 'special' prefix
  JFT_KeySize size;          // the number of stem bytes
  uint8_t *data;             // variable stem bytes
} JFT_Stem;

typedef struct {
  JFT_Count size;            // number of words in data
  JFT_Word *data;            // variable word list
} JFT_Leaf;

typedef struct {
  JFT *root;                 // the root of the trie (for cons)
  JFT *node;                 // the current node
  JFT_Stem stem;             // the stem of the node
  JFT_StemPos point;         // position within the stem (or after)
  JFT_Symbol symbol;         // actual symbol under the cursor
} JFT_Cursor;

typedef struct {
  JFT_Cursor *cursor;
  JFT_Stem *stem;
  JFT_KeySize zero;
  JFT_Direction direction;
  uintmax_t nth;
} JFT_Keys;

typedef struct {
  JFT_Count position;
  JFT_Count size;
  JFT_Word *data;
} JFT_Batch;

typedef struct Iter {
  JFT_Boolean (*next)(struct Iter *iter);
  JFT_Batch batch;
  JFT_Word words[JFT_ITER_WORDS];
  JFT_Word owner;
  union {
    struct {
      struct Iter *iters;
      JFT_Mask exists;
      JFT_Mask active;
    } many;
    struct {
      JFT_Leaf leaf;
      JFT_Offset *TLB;
      JFT_Count TLBSize;
      JFT_Count position;
      JFT_Count lower;
      JFT_Count upper;
    } trans;
  } sub;
} JFT_Iter;

typedef struct Buffer {
  int factor;
  size_t size;
  size_t mark;
  uint8_t *data;
  int (*ensure)(struct Buffer *buf, size_t more);
} JFT_Buffer;

typedef struct {
  JFT_Buffer *buffer;
  JFT_Symbol keySpace;
  JFT_Root root;
  JFT_Stem stem;
  JFT_Iter *iters;
  JFT_Offset **TLBs;
  JFT_Count *TLBSizes;
} JFT_Cons;

typedef struct {
  JFT_Symbol bound;
  JFT_Mask active;
  JFT_Mask done;
  JFT_Offset offset;
} JFT_MergeFrame;

typedef struct {
  JFT_MergeFrame *stack;
  JFT_MergeFrame *frame;
  JFT_KeySize maxKeySize;
  JFT_Amount numCursors;
  void *acc;
  int flags;
} JFT_MergeContext;

typedef JFT_Symbol (*JFT_CursorFun)(JFT_Cursor *cursor, JFT_Direction direction);
typedef JFT_Status (*JFT_SpliceFun)(JFT_Cursor *cursors, JFT_MergeContext *ctx, JFT_Phase phase);

/* Jump table ranges */

static const JFT_Range JumpTableRanges[] = {
  {.min = JFT_SYMBOL_PRIMARY,  // root
               .max = JFT_SYMBOL_INDICES},
  {.min = 65,  .max = 90},     // upper
  {.min = 97,  .max = 122},    // lower
  {.min = 48,  .max = 57},     // numeral
  {.min = 0,   .max = 31},     // control
#define JFT_JUMP_TABLE_BIG  5
  {.min = 65,  .max = 122},    // upper-lower
  {.min = 48,  .max = 122},    // numeral-lower
  {.min = 32,  .max = 127},    // printable
  {.min = 0,   .max = 127},    // ascii
  {.min = 128, .max = 255},    // extended / utf-8
  {.min = 0,   .max = 255}     // binary
#define JFT_JUMP_TABLE_END  11
};

/* Symbol sets */

static inline int JFT_count_bits(uint64_t v) {
  // i.e. https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
  v = (v - ((v >> 1) & (uint64_t)~(uint64_t)0 / 3));
  v = (v & (uint64_t)~(uint64_t)0 / 15 * 3) + ((v >> 2) & (uint64_t)~(uint64_t)0 / 15 * 3);
  v = (v + (v >> 4)) & (uint64_t)~(uint64_t)0 / 255 * 15;
  v = (v * ((uint64_t)~(uint64_t)0 / 255)) >> (sizeof(uint64_t) - 1) * 8;
  return v;
}

static inline int JFT_symbol_set_count(JFT_SymbolSet *set) {
  return (JFT_count_bits((*set)[0]) +
          JFT_count_bits((*set)[1]) +
          JFT_count_bits((*set)[2]) +
          JFT_count_bits((*set)[3]));
}

static inline JFT_SymbolSet *JFT_symbol_set_bit(JFT_SymbolSet *set, JFT_Symbol symbol) {
  return ((*set)[symbol / 64] |= (1LLU << (symbol % 64))), set;
}

/* Node head */

static inline JFT_Head JFT_head(JFT_NodeType nodeType,
                                JFT_Boolean isSpecial,
                                JFT_KeySize stemSize,
                                JFT_TypeInfo typeInfo,
                                JFT_Offset parentOffset) {
  return ((((JFT_Head)(nodeType)) << 62) |
          (((JFT_Head)(isSpecial > 0)) << 61) |
          (((JFT_Head)(stemSize & 0b111111111111)) << 48) |
          (((JFT_Head)(typeInfo & 0b11111111)) << 40) |
          (((JFT_Head)(parentOffset & 0xFFFFFFFFFF))));
}

static inline JFT_NodeType JFT_node_type(const JFT *trie) {
  return ((*(JFT_Head *)trie) >> 62);
}

static inline JFT_Boolean JFT_is_special(const JFT *trie) {
  return ((*(JFT_Head *)trie) >> 61) & 0b1;
}

static inline JFT_Boolean JFT_is_dirty(const JFT *trie) {
  return ((*(JFT_Head *)trie) >> 60) & 0b1;
}

static inline JFT_KeySize JFT_stem_size(const JFT *trie) {
  return ((*(JFT_Head *)trie) >> 48) & 0b111111111111;
}

static inline JFT_TypeInfo JFT_type_info(const JFT *trie) {
  return ((*(JFT_Head *)trie) >> 40) & 0b11111111;
}

static inline JFT_Offset JFT_parent_offset(const JFT *trie) {
  return ((*(JFT_Offset *)trie) & 0xFFFFFFFFFF);
}

static inline void JFT_set_dirty(JFT *trie, JFT_Boolean dirty) {
  *(JFT_Head *)trie =
    (*(JFT_Head *)trie & ~((JFT_Head)0b1 << 60)) |
    (((JFT_Head)dirty & 0b11111111) << 60);
}

static inline void JFT_set_type_info(JFT *trie, JFT_TypeInfo info) {
  *(JFT_Head *)trie =
    (*(JFT_Head *)trie & ~((JFT_Head)0b11111111 << 40)) |
    (((JFT_Head)info & 0b11111111) << 40);
}

static inline void JFT_set_parent_offset(JFT *trie, JFT_Offset offset) {
  *(JFT_Head *)trie =
    (*(JFT_Head *)trie & ~0xFFFFFFFFFF) |
    ((JFT_Head)offset & 0xFFFFFFFFFF);
}

/* Debug helpers */

static inline const char *JFT_node_type_name(const JFT *trie) {
  switch (JFT_node_type(trie)) {
    case Root:      return "Root";
    case ScanList:  return "ScanList";
    case JumpTable: return "JumpTable";
    case Leaf:      return "Leaf";
    default:        return "N/A";
  }
}

static inline const char *JFT_symbol_name(const JFT_Symbol *symbol) {
  switch (*symbol) {
    case JFT_SYMBOL_ANY:     return "ANY";
    case JFT_SYMBOL_NIL:     return "NIL";
    case JFT_SYMBOL_TOP:     return "TOP";
    case JFT_SYMBOL_PRIMARY: return "PRIMARY";
    case JFT_SYMBOL_INDICES: return "INDICES";
    default:                 return (const char *)symbol;
  }
}

/* Branch / Leaf only */

static inline const uint8_t *JFT_type_data(const JFT *trie) {
  return trie + sizeof(JFT_Head) + JFT_stem_size(trie);
}

static inline JFT_Stem JFT_stem(const JFT *trie) {
  if (JFT_is_special(trie))
    return (JFT_Stem) {
      .pre = JFT_BToSp(*(trie + sizeof(JFT_Head))),
      .size = JFT_stem_size(trie),
      .data = (uint8_t *)(trie + sizeof(JFT_Head) + 1)
    };
  return (JFT_Stem) {
    .size = JFT_stem_size(trie),
    .data = (uint8_t *)(trie + sizeof(JFT_Head))
  };
}

static inline JFT_Leaf JFT_leaf(const JFT *trie) {
  JFT_TypeInfo size = JFT_type_info(trie);
  const uint8_t *data = JFT_type_data(trie);
  if (size == 0xFF) {
    size = *(JFT_Count *)data;
    data += sizeof(JFT_Count);
  }
  return (JFT_Leaf) {
    .size = size,
    .data = (JFT_Word *)data
  };
}

/* Node sizing */

static inline JFT_Offset JFT_leaf_size(JFT_Count stemSize, JFT_Count leafSize) {
  return ((sizeof(JFT_Head)) +
          (stemSize) +
          (leafSize < 0xFF ? 0 : sizeof(JFT_Count)) +
          (leafSize * sizeof(JFT_Word)));
}

static inline JFT_Offset JFT_scan_size(JFT_Count stemSize, JFT_Count numSymbols) {
  return ((sizeof(JFT_Head)) +
          (stemSize) +
          (numSymbols * (1 + sizeof(JFT_Offset))));
}

static inline JFT_Offset JFT_jump_size(JFT_Count stemSize, JFT_Count rangeSize) {
  return ((sizeof(JFT_Head)) +
          (stemSize) +
          (rangeSize * sizeof(JFT_Offset)));
}

/* Trie traversal */

static inline JFT *JFT_parent(JFT *trie) {
  return trie - JFT_parent_offset(trie);
}

static inline JFT_Root *JFT_root(JFT *trie) {
  if (JFT_node_type(trie) == Root)
    return (JFT_Root *)trie;
  return JFT_root(JFT_parent(trie));
}

static inline JFT_Offset *JFT_scan_child_slot(const JFT *trie,
                                              JFT_Symbol symbol,
                                              int delta) {
  JFT_TypeInfo length = JFT_type_info(trie);
  const uint8_t *symbols = JFT_type_data(trie);
  JFT_Offset *offsets = (JFT_Offset *)(symbols + length);
  JFT_Offset lo = 0, hi = length, mid = 0;
  JFT_Symbol at = 0;

  // if looking for nil, return min, max, or nil (children can be missing, so search)
  if (symbol == JFT_SYMBOL_NIL) {
    if (delta > 0) {
      for (int i = lo; i < hi; i++)
        if (offsets[i])
          return offsets + i;
    } else if (delta < 0) {
      for (int i = hi - 1; i >= lo; i--)
        if (offsets[i])
          return offsets + i;
    }
    return 0;
  }

  // first binary search for the symbol, then adjust according to delta
  while (lo < hi) {
    mid = (lo + hi) / 2;
    at = *(symbols + mid);
    if (at < symbol)
      lo = mid + 1;
    else if (at > symbol)
      hi = mid;
    else
      break;
  }

  // we either found the symbol, or the bounds collapsed
  if (delta == 0) {
    // we want the symbol, only return if bounds are slack
    if (lo < hi)
      return offsets + mid;
  } else if (delta > 0) {
    // we want the symbol after, return either this or the next (if exists)
    if (at > symbol)
      return offsets + mid;
    else if (mid < length - 1)
      return offsets + mid + 1;
  } else if (delta < 0) {
    // we want the symbol before, return either this or the prev (if exists)
    if (at < symbol)
      return offsets + mid;
    else if (mid > 0)
      return offsets + mid - 1;
  }
  return NULL;
}

static inline JFT_Offset *JFT_jump_child_slot(const JFT *trie,
                                              JFT_Symbol symbol,
                                              int delta) {
  JFT_TypeInfo rangeId = JFT_type_info(trie);
  JFT_Offset *offsets = (JFT_Offset *)JFT_type_data(trie);
  JFT_Range range = JumpTableRanges[rangeId];
  JFT_Symbol min = range.min, max = range.max;

  // if its NIL, handle it first
  if (symbol == JFT_SYMBOL_NIL) {
    if (delta > 0)
      goto searchForward; // range.min, range.max
    if (delta < 0)
      goto searchReverse; // range.max, range.min
    return NULL;
  }

  // otherwise, figure out if its in the range of the table and search appropriately
  if (symbol < range.min) {
    if (delta > 0)
      goto searchForward; // range.min, range.max
  } else if (symbol <= range.max) {
    if (delta == 0) {
      return offsets + symbol - range.min;
    } else if (delta < 0) {
      max = symbol - 1;
      goto searchReverse; // symbol - 1, range.min
    } else if (delta > 0) {
      min = symbol + 1;
      goto searchForward; // symbol + 1, range.max
    }
  } else {
    if (delta < 0)
      goto searchReverse; // range.max, range.min
  }
  return NULL;

 searchForward:
  offsets += min - range.min;
  for (JFT_Symbol i = min; i <= max; i++, offsets++)
    if (*offsets)
      return offsets;
  return NULL;

 searchReverse:
  offsets += max - range.min;
  for (JFT_Symbol i = max; i >= min; i--, offsets--)
    if (*offsets)
      return offsets;
  return NULL;
}

static inline JFT_Offset JFT_maybe_offset(JFT_Offset *slot) {
  return slot ? *slot : 0;
}

static inline JFT_Offset JFT_child_offset(const JFT *trie,
                                          JFT_Symbol symbol,
                                          int delta) {
  switch (JFT_node_type(trie)) {
    case Root:      return JFT_maybe_offset(JFT_jump_child_slot(trie, symbol, delta));
    case ScanList:  return JFT_maybe_offset(JFT_scan_child_slot(trie, symbol, delta));
    case JumpTable: return JFT_maybe_offset(JFT_jump_child_slot(trie, symbol, delta));
    case Leaf:      return 0;
    default:        return 0; // no other types, impossible
  }
}

static inline JFT_Offset JFT_set_child_offset(JFT *trie,
                                              JFT_Symbol symbol,
                                              JFT_Offset offset) {
  JFT_Offset *slot = NULL;
  switch (JFT_node_type(trie)) {
    case Root:      slot = JFT_jump_child_slot(trie, symbol, 0); break;
    case ScanList:  slot = JFT_scan_child_slot(trie, symbol, 0); break;
    case JumpTable: slot = JFT_jump_child_slot(trie, symbol, 0); break;
    case Leaf:      /* leaf has no children, shouldn't happen */ break;
  }
  if (slot)
    *slot = offset;
  return offset;
}

static inline JFT *JFT_child(JFT *trie, JFT_Symbol symbol, int delta) {
  JFT_Offset offset = JFT_child_offset(trie, symbol, delta);
  return offset ? trie + offset : NULL;
}

static inline int JFT_mark_symbol(JFT_Range *range,
                                  JFT_SymbolSet *set,
                                  JFT_Symbol symbol) {
  if (symbol < range->min)
    range->min = symbol;
  if (symbol > range->max)
    range->max = symbol;
  JFT_symbol_set_bit(set, symbol);
  return 0;
}

static inline int JFT_mark_scan_children(const JFT *trie,
                                         JFT_Range *range,
                                         JFT_SymbolSet *set) {
  JFT_TypeInfo length = JFT_type_info(trie);
  const uint8_t *symbols = JFT_type_data(trie);
  for (int i = 0; i < length; i++)
    JFT_mark_symbol(range, set, symbols[i]);
  return 0;
}

static inline int JFT_mark_jump_children(const JFT *trie,
                                         JFT_Range *range,
                                         JFT_SymbolSet *set) {
  JFT_TypeInfo rangeId = JFT_type_info(trie);
  JFT_Offset *offsets = (JFT_Offset *)JFT_type_data(trie);
  JFT_Range table = JumpTableRanges[rangeId];
  for (JFT_Symbol s = table.min; s <= table.max; s++)
    if (offsets[s - table.min])
      JFT_mark_symbol(range, set, s);
  return 0;
}

static inline int JFT_mark_children(const JFT *trie,
                                    JFT_Range *range,
                                    JFT_SymbolSet *set) {
  switch (JFT_node_type(trie)) {
    case ScanList:  return JFT_mark_scan_children(trie, range, set);
    case JumpTable: return JFT_mark_jump_children(trie, range, set);
    default:        return -1; /* should not happen: error */
  }
}

/* Cursors */

JFT_Symbol JFT_cursor_init(JFT_Cursor *cursor, JFT_Phase phase, JFT *node);
JFT_Symbol JFT_cursor_back(JFT_Cursor *cursor, JFT_Direction direction);
JFT_Symbol JFT_cursor_step(JFT_Cursor *cursor, JFT_Direction direction);
JFT_Symbol JFT_cursor_next(JFT_Cursor *cursor, JFT_Direction direction);
JFT_Symbol JFT_cursor_find(JFT_Cursor *cursor, const JFT_Stem *stem);

JFT_Status JFT_cursor_merge(JFT_Cursor *cursors,
                            JFT_Amount num,
                            JFT_SpliceFun splice,
                            void *acc,
                            int flags);

JFT *JFT_cursor_merge_new(JFT_Cursor *cursors,
                          JFT_Amount num,
                          JFT_Buffer *output,
                          int flags);

static inline JFT_Symbol JFT_symbol_at_point(const JFT_Stem *stem, JFT_StemPos point) {
  // NB: assumes point is valid
  if (stem->pre)
    return point == 0 ? stem->pre : stem->data[point - 1];
  return stem->data[point];
}

static inline JFT_Symbol JFT_set_symbol_at_point(JFT_Stem *stem, JFT_StemPos point, JFT_Symbol symbol) {
  if (point == 0)
    return symbol > 255 ? (stem->pre = symbol) : (stem->pre = 0), (stem->data[0] = symbol);
  return stem->data[stem->pre ? point - 1 : point] = symbol;
}

static inline JFT_Symbol JFT_cursor_symbol_at_point(const JFT_Cursor *cursor) {
  return JFT_symbol_at_point(&cursor->stem, cursor->point);
}

static inline JFT_Symbol JFT_cursor_set_symbol_at_point(JFT_Cursor *cursor, JFT_Symbol symbol) {
  return JFT_set_symbol_at_point(&cursor->stem, cursor->point, symbol);
}

static inline int JFT_cursor_at_terminal(const JFT_Cursor *cursor) {
  return JFT_node_type(cursor->node) == Leaf && cursor->point == cursor->stem.size - 1;
}

static inline int JFT_cursor_mark_subsequent(const JFT_Cursor *cursor,
                                             JFT_Range *range,
                                             JFT_SymbolSet *set) {
  // NB: assumes stem is not empty
  if (cursor->point == cursor->stem.size - 1)
    return JFT_mark_children(cursor->node, range, set);
  return JFT_mark_symbol(range, set, JFT_symbol_at_point(&cursor->stem, cursor->point + 1));
}

static inline JFT_Cursor JFT_cursor(JFT *root) {
  JFT_Cursor cursor = (JFT_Cursor) {.root = root};
  JFT_cursor_init(&cursor, In, root);
  return cursor;
}

/* Key traversal */

static inline JFT_Keys JFT_keys(JFT_Cursor *cursor,
                                JFT_Stem *stem,
                                JFT_Direction direction) {
  // stem is both the prefix to match, and where to store the iterated keys
  JFT_cursor_find(cursor, stem);
  return (JFT_Keys) {
    .cursor = cursor,
    .stem = stem,
    .zero = stem->size,
    .direction = direction
  };
}

static inline JFT_Stem *JFT_keys_next_until(JFT_Keys *keys, const JFT_Symbol *stop) {
  // after the first pass, wind back to the last fork before pushing on
  // if we hit a wall, or go out of the range of the prefix, stop
 next:
  if (keys->nth++) {
    while (JFT_cursor_next(keys->cursor, keys->direction) >= JFT_SYMBOL_NIL)
      if (!keys->stem->size-- || JFT_cursor_back(keys->cursor, keys->direction) >= JFT_SYMBOL_TOP)
        return NULL;
    if (keys->stem->size <= keys->zero)
      return NULL;
    JFT_set_symbol_at_point(keys->stem, keys->stem->size - 1, keys->cursor->symbol);
  }
  // append each symbol to the stem until we hit a stopping condition
  // in a 'degenerate' trie we might step off a cliff, in which case we must search for siblings
  // NB: its not *really* degenerate, its possible for branches to have all their children deleted
  //     eventually the branch should be removed too, but it can and does occur
  while (1) {
    if (stop && keys->cursor->symbol == *stop)
      return keys->stem;
    if (JFT_cursor_at_terminal(keys->cursor))
      return keys->stem;
    if (JFT_cursor_step(keys->cursor, keys->direction) >= JFT_SYMBOL_NIL) {
      if (JFT_cursor_back(keys->cursor, keys->direction) >= JFT_SYMBOL_TOP)
        return NULL;
      else
        goto next;
    }
    JFT_set_symbol_at_point(keys->stem, keys->stem->size++, keys->cursor->symbol);
  }
}

static inline JFT_Stem *JFT_keys_next(JFT_Keys *keys) {
  return JFT_keys_next_until(keys, NULL);
}

static inline JFT_Stem JFT_key(const JFT *trie, uint8_t *keyData) {
  JFT_Stem key = (JFT_Stem) {.data = keyData + JFT_KEY_LIMIT};
  for (JFT *node = (JFT *)trie; JFT_node_type(node) != Root; node = JFT_parent(node)) {
    JFT_Stem stem = JFT_stem(node);
    JFT_KeySize n = stem.size - !!stem.pre;
    key.pre = stem.pre;
    key.size += stem.size;
    key.data = memcpy(key.data - n, stem.data, n);
  }
  return key;
}

static inline JFT_Stem JFT_key_copy(JFT_Stem *key, uint8_t *keyData) {
  return (JFT_Stem) {
    .pre = key->pre,
    .size = key->size,
    .data = memcpy(keyData, key->data, key->size - !!key->pre)
  };
}

/* Iterators */

JFT_Boolean JFT_iter_leaf_next(JFT_Iter *iter);
JFT_Boolean JFT_iter_trans_next(JFT_Iter *iter);
JFT_Boolean JFT_iter_any_next(JFT_Iter *iter);
JFT_Boolean JFT_iter_all_next(JFT_Iter *iter);
JFT_Boolean JFT_iter_but_next(JFT_Iter *iter);

static inline JFT_Count JFT_iter_next(JFT_Iter *iter) {
  iter->batch.position = 0;
  return iter->next(iter);
}

static inline JFT_Iter JFT_iter_leaf(JFT_Leaf leaf) {
  return (JFT_Iter) {
    .next = JFT_iter_leaf_next,
    .batch = (JFT_Batch) {
      .position = 0,
      .size = leaf.size,
      .data = leaf.data
    }
  };
}

static inline JFT_Iter JFT_iter_none() {
  return JFT_iter_leaf((JFT_Leaf) {});
}

static inline JFT_Iter JFT_iter_trans(JFT_Leaf leaf, JFT_Offset *TLB, JFT_Count TLBSize) {
  return (JFT_Iter) {
    .next = JFT_iter_trans_next,
    .sub = {
      .trans = {
        .leaf = leaf,
        .TLB = TLB,
        .TLBSize = TLBSize,
        .lower = 0,
        .upper = -1,
      }
    }
  };
}

static inline JFT_Iter JFT_iter_any(JFT_Iter *iters, JFT_Mask active) {
  return (JFT_Iter) {
    .next = JFT_iter_any_next,
    .sub = {
      .many = {
        .iters = iters,
        .exists = active,
        .active = active
      }
    }
  };
}

static inline JFT_Iter JFT_iter_all(JFT_Iter *iters, JFT_Mask active) {
  return (JFT_Iter) {
    .next = JFT_iter_all_next,
    .sub = {
      .many = {
        .iters = iters,
        .exists = active,
        .active = active
      }
    }
  };
}

static inline JFT_Iter JFT_iter_but(JFT_Iter *iters, JFT_Mask active) {
  return (JFT_Iter) {
    .next = JFT_iter_but_next,
    .sub = {
      .many = {
        .iters = iters,
        .exists = active,
        .active = active
      }
    }
  };
}

/* Buffers */

#define JFT_buffer_equal(buf, type, obj)                                         \
  ((*(type *)(buf->data + buf->mark) = (obj)), buf->mark += sizeof(type))

static inline size_t JFT_buffer_paste(JFT_Buffer *buf, void *ptr, size_t size) {
  memcpy(buf->data + buf->mark, ptr, size);
  return buf->mark += size;
}

static inline JFT_Buffer *JFT_buffer_ample(JFT_Buffer *buf, size_t more) {
  return buf->ensure(buf, more) == Ok ? buf : NULL;
}

static inline JFT_Buffer *JFT_buffer_reset(JFT_Buffer *buf) {
  buf->mark = 0;
  return buf;
}

static inline JFT_Buffer *JFT_buffer_write(JFT_Buffer *buf, void *ptr, size_t size) {
  if (!JFT_buffer_ample(buf, size))
    return NULL;
  JFT_buffer_paste(buf, ptr, size);
  return buf;
}

JFT_Buffer *JFT_membuf();
JFT_Buffer *JFT_membuf_free(JFT_Buffer *buf);

/* Atoms / Operations */

JFT *JFT_atom(const JFT_Stem *primary,
              const JFT_Leaf *leaf,
              const JFT_Stem *indices,
              JFT_Count numIndices,
              JFT_Buffer *scratch,
              JFT_Buffer *output);

#endif /* __JFT_H__ */
