#include <stdlib.h>
#include <string.h>

#include "trie.h"

#define BUF_CLR(buf)       (buf->mark = 0, buf)
#define BUF_HAS(buf, more) (buf->ensure(buf, more) == Ok ? buf : NULL)
#define BUF_SET(buf, type, obj)                                         \
  ((*(type *)(buf->data + buf->mark) = (obj)), buf->mark += sizeof(type))
#define BUF_CPY(buf, ptr, size)                                         \
  (memcpy(buf->data + buf->mark, (ptr), size), buf->mark += size)

JFT_Symbol JFT_cursor_init(JFT_Cursor *cursor, JFT_Phase phase, JFT *node) {
  // initialize the cursor at node, if it exists, and return the symbol
  // we assume non-root branches always have children
  // it is impossible to construct such a node without children
  // we assume non-root nodes cannot have an empty stem
  // although technically it is possible to construct such a node, it would be invalid
  if (node == NULL)
    return cursor->symbol = JFT_SYMBOL_NIL;
  switch (JFT_node_type(node)) {
    case Root:
      cursor->node = node;
      cursor->stem = (JFT_Stem) {0};
      cursor->point = 0;
      cursor->symbol = JFT_SYMBOL_TOP;
      break;
    default:
      cursor->node = node;
      cursor->stem = JFT_stem(node);
      cursor->point = phase == In ? 0 : cursor->stem.size - 1;
      cursor->symbol = JFT_cursor_symbol_at_point(cursor);
      break;
  }
  return cursor->symbol;
}

JFT_Symbol JFT_cursor_back(JFT_Cursor *cursor, JFT_Direction direction) {
  if (--cursor->point < 0 || cursor->stem.size == 0)
    return JFT_cursor_init(cursor, Out, JFT_parent(cursor->node));
  return cursor->symbol = JFT_cursor_symbol_at_point(cursor);
}

JFT_Symbol JFT_cursor_step(JFT_Cursor *cursor, JFT_Direction direction) {
  if (cursor->symbol == JFT_SYMBOL_NIL)
    return cursor->symbol;
  if (++cursor->point >= cursor->stem.size)
    switch (JFT_node_type(cursor->node)) {
      case Leaf:
        return cursor->symbol = JFT_SYMBOL_ANY;
      default:
        return JFT_cursor_init(cursor, In, JFT_child(cursor->node, JFT_SYMBOL_NIL, direction));
    }
  return cursor->symbol = JFT_cursor_symbol_at_point(cursor);
}

JFT_Symbol JFT_cursor_next(JFT_Cursor *cursor, JFT_Direction direction) {
  if (cursor->symbol == JFT_SYMBOL_ANY)
    return cursor->symbol;
  if (cursor->point == 0)
    return JFT_cursor_init(cursor, In, JFT_child(JFT_parent(cursor->node), cursor->symbol, direction));
  if (cursor->symbol == JFT_SYMBOL_NIL && cursor->point < cursor->stem.size)
    return cursor->symbol = JFT_cursor_symbol_at_point(cursor);
  return cursor->symbol = JFT_SYMBOL_NIL;
}

JFT_Symbol JFT_cursor_find(JFT_Cursor *cursor, const JFT_Stem *stem) {
  if (stem->pre && stem->size)
    if (JFT_cursor_init(cursor, In, JFT_child(cursor->node, stem->pre, 0)) == JFT_SYMBOL_NIL)
      return cursor->symbol;
  JFT_KeySize size = stem->pre && stem->size ? stem->size - 1 : stem->size;
  for (JFT_KeySize i = 0; i < size; i++) {
    if (cursor->symbol == JFT_SYMBOL_NIL)
      return cursor->symbol;
    if (++cursor->point >= cursor->stem.size)
      switch (JFT_node_type(cursor->node)) {
        case Leaf:
          return cursor->symbol = JFT_SYMBOL_ANY;
        default:
          JFT_cursor_init(cursor, In, JFT_child(cursor->node, stem->data[i], 0));
      }
    if ((cursor->symbol = JFT_cursor_symbol_at_point(cursor)) != stem->data[i])
      return cursor->symbol = JFT_SYMBOL_NIL;
  }
  return cursor->symbol;
}

static JFT_KeySize max_key_size(JFT_Cursor *cursors, JFT_Amount num) {
  JFT_KeySize cur, max = 0;
  for (JFT_Amount i = 0; i < num; i++)
    if ((cur = JFT_root(cursors[i].node)->maxKeySize) > max)
      max = cur;
  return max + 1; // + top
}

static JFT_MergeFrame *move_into_frame(JFT_Cursor *cursors,
                                       JFT_CursorFun advance,
                                       JFT_Mask relevant,
                                       JFT_MergeFrame *frame,
                                       JFT_Direction direction) {
  // compute the new active set and bound from the relevant cursors
  // advance the active cursors (assumed to be a subset of relevant)
  JFT_Symbol bound = JFT_SYMBOL_NIL, symbol;
  JFT_Mask definite = 0, possible = 0, preclude = 0;
  for (JFT_Amount b = ffsll(relevant); b; b = ffsll(relevant &= relevant - 1)) {
    JFT_Amount i = b - 1;
    JFT_Mask m = 1LLU << i;
    if (frame->active & m)
      advance(&cursors[i], direction);
    symbol = cursors[i].symbol;
    if (symbol == JFT_SYMBOL_NIL) {
      // the symbol is NIL: not active and has no more siblings
      preclude |= m;
    } else if (symbol == JFT_SYMBOL_ANY) {
      // the symbol is ANY: definitely active, but does not tighten the bound
      definite |= m;
    } else if (symbol == bound) {
      // the symbol matches the bound: potentially include it in active set
      possible |= m;
    } else if ((direction == Forward && symbol < bound) ||
               (direction == Reverse && symbol > bound) ||
               (bound == JFT_SYMBOL_NIL)) {
      // the symbol beats the bound: replace the potential active set
      bound = symbol;
      possible = m;
    }
  }
  frame->bound = bound;
  frame->active = possible | definite;
  frame->done = preclude;
  return frame;
}

static JFT_MergeFrame *back_into_frame(JFT_Cursor *cursors,
                                       JFT_MergeFrame *frame,
                                       JFT_Direction direction) {
  // back out all of the cursors which were active in the frame
  // frame is assumed to be the parent frame
  JFT_Mask active = frame->active;
  for (JFT_Amount b = ffsll(active); b; b = ffsll(active &= active - 1))
    JFT_cursor_back(&cursors[b - 1], direction);
  return frame;
}

JFT_Status JFT_cursor_merge(JFT_Cursor *cursors,
                            JFT_Amount num,
                            JFT_SpliceFun splice,
                            void *restrict acc,
                            int flags) {
  // cursors are assumed to be at the same initial location (i.e. root)
  // we currently support merging up to 64 cursors at a time
  // JFDB cannot have more than 64 levels, so internally we don't need more
  // more than that and caller can merge in batches
  if (num < 1 || num > JFT_MASK_CAPACITY)
    return ENumCursors;

  // allocate all the stack space we will need up front
  JFT_KeySize maxKeySize = max_key_size(cursors, num);
  JFT_MergeFrame *stack = calloc(maxKeySize, sizeof(JFT_MergeFrame)), *frame = stack;
  if (stack == NULL)
    return ENoMem;

  // initialize stack, etc.
  JFT_Status result = Ok;
  JFT_Direction direction = (flags & JFT_FLAGS_REVERSE) ? Reverse : Forward;
  *frame = (JFT_MergeFrame) {
    .bound = JFT_SYMBOL_ANY,
    .active = JFT_MASK_ACTIVE(num),
    .done = 0
  };

  JFT_MergeContext ctx = (JFT_MergeContext) {
    .stack = stack,
    .frame = frame,
    .maxKeySize = maxKeySize,
    .numCursors = num,
    .acc = acc,
    .flags = flags
  };

  // walk all the cursors in parallel, keeping them in sync
  // the point is we visit each path in the merged-trie exactly once
  while (1) {
    // are there any active cursors with discrete symbols?
    while (frame->active && frame->bound != JFT_SYMBOL_NIL) {
      // merge the active set
      switch (result = splice(cursors, &ctx, In)) {
        case Step:
          // push a new frame, get subsequent symbol from all active cursors
          // copy the currently active cursors and offset into the new frame
          (frame + 1)->active = frame->active;
          (frame + 1)->offset = frame->offset;
          frame = ctx.frame = move_into_frame(cursors,
                                              JFT_cursor_step,
                                              frame->active,
                                              frame + 1,
                                              direction);
          break;
        case Next:
          // shift the frame by recomputing active cursors
          // i.e. from those that stepped into this frame that are not done
          // if at the root, there can be no siblings, so we are done
          // reset the offset to that of the parent (in case prev changed it)
          if (frame == stack)
            goto out;
          frame->offset = (frame - 1)->offset;
          frame = ctx.frame = move_into_frame(cursors,
                                              JFT_cursor_next,
                                              (frame - 1)->active & ~frame->done,
                                              frame,
                                              direction);
          break;
        default:
          // early exit, pass the result through
          goto done;
      }
    }

    // we hit a wall, start backing out: pop the frame, call second splice
    // its not possible we are at the root, we would have exited already
    frame = ctx.frame = back_into_frame(cursors, frame - 1, direction);
  out:
    result = splice(cursors, &ctx, Out);

    // we could be at the root now, if so pass the result through
    // otherwise, move on to the next sibling
    if (frame == stack)
      goto done;
    frame->offset = (frame - 1)->offset;
    frame = ctx.frame = move_into_frame(cursors,
                                        JFT_cursor_next,
                                        (frame - 1)->active & ~frame->done,
                                        frame,
                                        direction);
  }

 done:
  free(stack);
  return result;
}

/* Node construction */

static inline void write_node(JFT_Buffer *buf,
                              JFT_NodeType nodeType,
                              const JFT_Stem *stem,
                              JFT_TypeInfo typeInfo,
                              JFT_Offset parentOffset) {
  // take care of the common node head + stem details for leaves & branches
  if (stem->pre) {
    BUF_SET(buf, JFT_Head, JFT_head(nodeType, stem->pre, stem->size, typeInfo, parentOffset));
    BUF_SET(buf, uint8_t, JFT_SpToB(stem->pre));
    BUF_CPY(buf, stem->data, stem->size - 1);
  } else {
    BUF_SET(buf, JFT_Head, JFT_head(nodeType, stem->pre, stem->size, typeInfo, parentOffset));
    BUF_CPY(buf, stem->data, stem->size);
  }
}

static inline JFT_Offset offset_parent(JFT_Buffer *buf,
                                       JFT_Offset parentPos,
                                       const JFT_Stem *stem) {
  // once we are ready to write the child, its parent slot can be updated, so do so
  return JFT_set_child_offset(buf->data + parentPos,
                              JFT_symbol_at_point(stem, 0),
                              buf->mark - parentPos);
}

static inline JFT_Offset make_root(JFT_Buffer *buf) {
  // just make space, as a root head initializes to nil (technically an impl detail)
  if (!BUF_HAS(buf, sizeof(JFT_Root)))
    return 0;
  return BUF_SET(buf, JFT_Root, (JFT_Root) {.head = JFT_head(Root, False, 0, 0, 0)});
}

static inline JFT_Offset make_scan_list(JFT_Buffer *buf,
                                        JFT_Offset parentPos,
                                        const JFT_Stem *stem,
                                        JFT_SymbolSet *symbols,
                                        int numSymbols) {
  // first create the list of symbols for searching, then the list of offsets
  JFT_Offset size = JFT_scan_size(stem->size, numSymbols);
  if (!BUF_HAS(buf, size))
    return 0;

  JFT_Offset parentOffset = offset_parent(buf, parentPos, stem);
  write_node(buf, ScanList, stem, numSymbols, parentOffset);

  uint64_t word;
  for (int w = 0; w < 4; w++)
    for (int b = ffsll(word = (*symbols)[w]); b; b = ffsll(word &= word - 1))
      BUF_SET(buf, uint8_t, 64 * w + b - 1);

  for (int i = 0; i < numSymbols; i++)
    BUF_SET(buf, JFT_Offset, 0);

  return size;
}

static inline JFT_Offset make_jump_table(JFT_Buffer *buf,
                                         JFT_Offset parentPos,
                                         const JFT_Stem *stem,
                                         JFT_TypeInfo rangeId) {
  // just create an offset for each symbol in the range
  int rangeSize = JumpTableRanges[rangeId].max - JumpTableRanges[rangeId].min + 1;
  JFT_Offset size = JFT_jump_size(stem->size, rangeSize);
  if (!BUF_HAS(buf, size))
    return 0;

  JFT_Offset parentOffset = offset_parent(buf, parentPos, stem);
  write_node(buf, JumpTable, stem, rangeId, parentOffset);

  for (int i = 0; i < rangeSize; i++)
    BUF_SET(buf, JFT_Offset, 0);

  return size;
}

static inline JFT_Offset make_branch(JFT_Buffer *buf,
                                     JFT_Offset parentPos,
                                     const JFT_Stem *stem,
                                     const JFT_Range *range,
                                     JFT_SymbolSet *symbols,
                                     int numSymbols) {
  // figure out what kind of branch to make
  if (numSymbols < 16)
    return make_scan_list(buf, parentPos, stem, symbols, numSymbols);

  for (int i = 1; i < JFT_JUMP_TABLE_BIG; i++)
    if (range->min >= JumpTableRanges[i].min &&
        range->max <= JumpTableRanges[i].max)
      return make_jump_table(buf, parentPos, stem, i);

  if (numSymbols < 32)
    return make_scan_list(buf, parentPos, stem, symbols, numSymbols);

  for (int i = JFT_JUMP_TABLE_BIG; i < JFT_JUMP_TABLE_END; i++)
    if (range->min >= JumpTableRanges[i].min &&
        range->max <= JumpTableRanges[i].max)
      return make_jump_table(buf, parentPos, stem, i);

  return make_scan_list(buf, parentPos, stem, symbols, numSymbols);

}

static inline JFT_Offset prep_leaf(JFT_Buffer *buf,
                                   JFT_Offset parentPos,
                                   const JFT_Stem *stem,
                                   JFT_Count maxWords) {
  // write the node, although it may be updated later
  // ensure space for maxWords and return a pointer where one can write words
  JFT_Offset size = JFT_leaf_size(stem->size, maxWords);
  if (!BUF_HAS(buf, size))
    return 0;

  JFT_Offset parentOffset = offset_parent(buf, parentPos, stem);

  if (maxWords < 0xFF) {
    write_node(buf, Leaf, stem, maxWords, parentOffset);
  } else {
    write_node(buf, Leaf, stem, 0xFF, parentOffset);
    BUF_SET(buf, JFT_Count, maxWords);
  }

  return size;
}

static inline void edit_leaf(JFT_Buffer *buf,
                             JFT_Offset pos,
                             const JFT_Stem *stem,
                             JFT_Count maxWords,
                             JFT_Count numWords) {
  // correct the actual number of words written
  // if we were using the overflow size area, we will continue to use it
  // even if the actual number of words is small enough to fit now
  if (maxWords < 0xFF)
    JFT_set_type_info(buf->data + pos, numWords);
  else
    *(JFT_Count *)(buf->data + pos + sizeof(JFT_Head) + stem->size) = numWords;
}

static inline JFT_Offset make_leaf(JFT_Buffer *buf,
                                   JFT_Offset parentPos,
                                   const JFT_Stem *stem,
                                   const JFT_Leaf *leaf) {
  // write the node and copy all the leaf data words into the buffer
  JFT_Offset size = prep_leaf(buf, parentPos, stem, leaf->size);
  for (int i = 0; i < leaf->size; i++)
    BUF_SET(buf, JFT_Word, leaf->data[i]);
  return size;
}

static JFT_Status splice_new(JFT_Cursor *cursors,
                             JFT_MergeContext *ctx,
                             JFT_Phase phase) {
  // determine what to do based on the most recent / highest precedence cursor
  // (cursors should be given in order of recency / precedence)
  JFT_MergeFrame *frame = ctx->frame;
  JFT_Mask active = frame->active;
  JFT_Amount b = ffsll(active);
  JFT_Cursor *cursor = cursors + b - 1;
  JFT_Cons *cons = (JFT_Cons *)ctx->acc;
  JFT_Buffer *buf = cons->buffer;
  JFT_NodeType type = JFT_node_type(cursor->node);

  if (phase == Out) {
    // do nothing during phase out, except finishing touches
    // copy the changed root data back into the buffer
    if (type == Root) {
      JFT_Root *root = (JFT_Root *)buf->data;
      root->extent = buf->mark;
      root->numPrimary = cons->root.numPrimary;
      root->numIndices = cons->root.numIndices;
      root->maxIndices = cons->root.maxIndices;
      root->maxKeySize = ctx->maxKeySize;
    }
    return Ok;
  }

  if (type == Root) {
    // on the root: make space for the new root in buffer
    // root is a parent, mark it's offset before writing
    frame->offset = buf->mark;
    if (!make_root(buf))
      return ENoMem;

    // keep a root in cons and update it later (NB: cannot rely on buf pointers)
    // allocate memory to store the compressed stem before we write it
    cons->root.head = JFT_head(Root, False, 0, 0, 0);
    cons->stem = (JFT_Stem) {.data = malloc(ctx->maxKeySize)};
    if (cons->stem.data == NULL)
      return ENoMem;

    // allocate the iterators for indices
    cons->iters = malloc(ctx->numCursors * sizeof(JFT_Iter));

    // allocate the offset translation tables for indices (if we are translating)
    if (!(ctx->flags & JFT_FLAGS_ATOM)) {
      cons->TLBs = malloc(ctx->numCursors * sizeof(JFT_Offset *));
      cons->TLBSizes = calloc(ctx->numCursors, sizeof(JFT_Count));
      for (JFT_Amount k = 0; k < ctx->numCursors; k++)
        cons->TLBs[k] = malloc(2 * ((JFT_Root *)cursors[k].root)->numPrimary * sizeof(JFT_Offset));
    }

    // step into the tree
    return Step;
  }

  // add the symbol to the budding stem
  // if its a keyspace, mark the keyspace in the cons
  if (cursor->symbol == JFT_SYMBOL_PRIMARY ||
      cursor->symbol == JFT_SYMBOL_INDICES) {
      cons->stem.pre = cons->keySpace = cursor->symbol;
      cons->stem.size++;
  } else {
      cons->stem.data[cons->stem.size - (cons->stem.pre ? 1 : 0)] = cursor->symbol;
      cons->stem.size++;
  }

  // if cursor is on a terminal node: overwrite
  if (JFT_cursor_at_terminal(cursor)) {
    int doTranslate = !(ctx->flags & JFT_FLAGS_ATOM);
    JFT_Leaf leaf = JFT_leaf(cursor->node);
    // simply skip empty leaves: the offset in parent will remain zero and thus will not branch
    // eventually (next merge), the node will get phased out of the trie completely
    if ((ctx->flags & JFT_FLAGS_PRUNE) && leaf.size == 0)
      return Next;

    if (cons->keySpace == JFT_SYMBOL_PRIMARY) {
      // at value node
      // if we are translating, store the translation of old value offset to new
      if (doTranslate) {
        cons->TLBs[b - 1][cons->TLBSizes[b - 1]++] = cursor->node - cursor->root;
        cons->TLBs[b - 1][cons->TLBSizes[b - 1]++] = buf->mark;
      }

      // emit the leaf node
      frame->offset = buf->mark;
      if (!make_leaf(buf, (frame - 1)->offset, &cons->stem, &leaf))
        return ENoMem;

      cons->root.numPrimary++;
    } else {
      // at index node
      // wrap each cursors' words in an iterator
      // which yields batches of sorted (maybe translated) words
      JFT_Count maxWords = 0, numWords = 0;
      do {
        cursor = cursors + b - 1;
        leaf = JFT_leaf(cursor->node);
        if (!(JFT_cursor_at_terminal(cursor)))
          break;
        if (doTranslate) {
          // use a translation iterator, translates a static size batch at a time
          cons->iters[b - 1] = JFT_iter_trans(leaf, cons->TLBs[b - 1], cons->TLBSizes[b - 1]);
        } else {
          // use an iterator that just returns the raw leaf words in one batch
          cons->iters[b - 1] = JFT_iter_leaf(leaf);
        }
        maxWords += leaf.size;
      } while ((b = ffsll(active &= active - 1)));

      // emit the leaf node using an upper bound on the number of words
      frame->offset = buf->mark;
      if (!(prep_leaf(buf, (frame - 1)->offset, &cons->stem, maxWords)))
        return ENoMem;

      // write the indices, count the actual words
      JFT_Iter i = JFT_iter_any(cons->iters, frame->active ^ active);
      do {
        BUF_CPY(buf, i.batch.data, i.batch.size * sizeof(JFT_Word));
        numWords += i.batch.size;
      } while (JFT_iter_next(&i));

      // correct the leaf node using the actual number of words
      edit_leaf(buf, frame->offset, &cons->stem, maxWords, numWords);

      if (numWords > cons->root.maxIndices)
        cons->root.maxIndices = numWords;
      cons->root.numIndices++;
    }

    // reset the bud
    cons->stem.pre = 0;
    cons->stem.size = 0;

    // done with this subtree
    return Next;
  }

  // cursor is non-terminal, maybe emit a branch
  // look at the other cursors and count # unique subsequent symbols
  JFT_Range range = {.min = 255, .max = 0};
  JFT_SymbolSet set = {};
  int count;
  do {
    if (cursor->symbol != JFT_SYMBOL_ANY)
      JFT_cursor_mark_subsequent(cursor, &range, &set);
    b = ffsll(active &= active - 1);
  } while (b && !JFT_cursor_at_terminal(cursor = cursors + b - 1));

  if ((count = JFT_symbol_set_count(&set)) > 1) {
    // more than one possible symbol: emit a branch node
    frame->offset = buf->mark;
    if (!make_branch(buf, (frame - 1)->offset, &cons->stem, &range, &set, count))
      return ENoMem;

    // reset the bud
    cons->stem.pre = 0;
    cons->stem.size = 0;
  }
  return Step;
}

JFT *JFT_cursor_merge_new(JFT_Cursor *cursors,
                          JFT_Amount num,
                          JFT_Buffer *output,
                          int flags) {
  // perform a cursor merge to produce a new trie
  // if 'atoms' are the base case, this is the induction step
  JFT_Cons cons = (JFT_Cons) {.buffer = BUF_CLR(output)};
  JFT_Status result = JFT_cursor_merge(cursors, num, splice_new, &cons, flags);
  free(cons.stem.data);
  free(cons.iters);
  if (cons.TLBs) {
    for (JFT_Amount k = 0; k < num; k++)
      free(cons.TLBs[k]);
    free(cons.TLBs);
  }
  free(cons.TLBSizes);
  if (result != Ok)
    return NULL;
  return (JFT *)output->data;
}

JFT *JFT_atom(const JFT_Stem *restrict primary,
              const JFT_Leaf *restrict value,
              const JFT_Stem *restrict indices,
              JFT_Count numIndices,
              JFT_Buffer *restrict scratch,
              JFT_Buffer *restrict output) {
  // create an atomic trie (i.e. a single operation)
  // using these atoms we can make bigger and bigger tries
  // first, manually create a trie for each individual key
  JFT_Count num = numIndices + 1;
  JFT_Offset rootOffsets[num];
  JFT_Root *root;
  JFT_Stem stem = (JFT_Stem) {
    .pre = JFT_SYMBOL_PRIMARY,
    .size = primary->size + !primary->pre,
    .data = primary->data
  };
  if (!(BUF_CLR(scratch)))
    return NULL;
  if (!(make_root(scratch)))
    return NULL;
  if (!(make_leaf(scratch, 0, &stem, value)))
    return NULL;
  root = (JFT_Root *)(scratch->data);
  root->extent = scratch->mark;
  root->numPrimary = 1;
  root->maxKeySize = stem.size;
  rootOffsets[0] = 0;

  JFT_Leaf index = JFTV(root->primaryOffset);
  for (JFT_Count i = 1; i < num; i++) {
    rootOffsets[i] = scratch->mark;
    stem.pre = JFT_SYMBOL_INDICES;
    stem.size = indices[i - 1].size + !indices[i - 1].pre;
    stem.data = indices[i - 1].data;
    if (!(make_root(scratch)))
      return NULL;
    if (!(make_leaf(scratch, rootOffsets[i], &stem, &index)))
      return NULL;
    root = (JFT_Root *)(scratch->data + rootOffsets[i]);
    root->extent = scratch->mark - rootOffsets[i];
    root->numIndices = 1;
    root->maxIndices = 1;
    root->maxKeySize = stem.size;
  }

  JFT_Cursor cursors[num];
  for (JFT_Amount i = 0; i < num; i++)
    cursors[i] = JFT_cursor((JFT *)(scratch->data + rootOffsets[i]));
  return JFT_cursor_merge_new(cursors, num, output, JFT_FLAGS_ATOM);
}

/* Iterators */

JFT_Boolean JFT_iter_leaf_next(JFT_Iter *iter) {
  return False;
}

JFT_Boolean JFT_iter_trans_next(JFT_Iter *iter) {
  // translate the next batch of offsets in the leaf
  // we are done once the position reaches the leaf size
  JFT_Leaf *leaf = &iter->sub.trans.leaf;
  JFT_Count left = leaf->size - iter->sub.trans.position;
  if (!left)
    return False;

  JFT_Word *words = iter->batch.data = iter->words;
  JFT_Word last = leaf->data[leaf->size - 1];
  JFT_Count lower = iter->sub.trans.lower;
  JFT_Count upper = MIN(iter->sub.trans.upper, iter->sub.trans.TLBSize / 2);
  for (iter->batch.size = 0; iter->batch.size < JFT_ITER_WORDS; ) {
    // try to make an informed initial guess
    // if that fails, fall back to bisection
    // NB: we could keep making 'informed' guesses, but it requires more work
    //    (because once we guess too high we no longer know how many are in range)
    JFT_Word word = leaf->data[iter->sub.trans.position++], at;
    JFT_Count lo = lower, hi = upper, mid = lo + (hi - lo) / (left + 1);
    while (lo < hi) {
      at = iter->sub.trans.TLB[2 * mid];
      if (at < word) {
        lo = mid + 1;
      } else if (at > word) {
        hi = mid;
        if (at > last)
          upper = mid;
      } else {
        lower = mid;
        break;
      }
      mid = lo + (hi - lo) / 2;
    }

    // we either found the word, or the bounds collapsed
    // if we found it, add the translated value
    // if the bounds collapsed, we won't be finding any more
    if (lo < hi)
      words[iter->batch.size++] = iter->sub.trans.TLB[2 * mid + 1];
    else
      iter->sub.trans.position = leaf->size;

    // once we are out of leaf words, we are done
    if (!(left = leaf->size - iter->sub.trans.position))
      break;
  }

  // remember the bounds for next time
  iter->sub.trans.lower = lower;
  iter->sub.trans.upper = upper;

  return True;
}

JFT_Boolean JFT_iter_any_next(JFT_Iter *iter) {
  // if all iterators are exhausted we are done
  if (!iter->sub.many.active)
    return False;

  JFT_Word *words = iter->batch.data = iter->words;
  for (iter->batch.size = 0; iter->batch.size < JFT_ITER_WORDS; ) {
    // scour the active iters for the next min element, until we fill a batch
    JFT_Mask hasMin = 0;
    JFT_Word min = -1, elem;
    JFT_Mask active = iter->sub.many.active;
    for (int b = ffsll(active); b; b = ffsll(active &= active - 1)) {
      JFT_Amount k = b - 1;
      JFT_Iter *i = iter->sub.many.iters + k;
      // as long as the position is out of bounds on the iter, reload it
      // if the iter is exhausted, remove it from active and skip to next
      while (i->batch.position >= i->batch.size)
        if (!JFT_iter_next(i)) {
          iter->sub.many.active &= ~(1LLU << k);
          goto nextActive;
        }
      if ((elem = i->batch.data[i->batch.position]) == min) {
        hasMin |= (1LLU << k);
      } else if (elem < min) {
        min = elem;
        hasMin = (1LLU << k);
      }
    nextActive:
      /* done with the active iter, move on to next */;
    }
    // if we can't find another min, all iters are exhausted and we are done
    // add the min to the batch always (main difference b/w any & all)
    if (!hasMin)
      break;
    else
      words[iter->batch.size++] = min;
    // advance any iters we found the min on
    for (int b = ffsll(hasMin); b; b = ffsll(hasMin &= hasMin - 1))
      iter->sub.many.iters[b - 1].batch.position++;
  }
  return True;
}

JFT_Boolean JFT_iter_all_next(JFT_Iter *iter) {
  // if any iterators are exhausted, we are done
  if (!iter->sub.many.active || iter->sub.many.active != iter->sub.many.exists)
    return False;

  JFT_Word *words = iter->batch.data = iter->words;
  for (iter->batch.size = 0; iter->batch.size < JFT_ITER_WORDS; ) {
    // scour the active iters for the next min element, until we fill a batch
    JFT_Mask hasMin = 0;
    JFT_Word min = -1, elem;
    JFT_Mask active = iter->sub.many.active;
    for (int b = ffsll(active); b; b = ffsll(active &= active - 1)) {
      JFT_Amount k = b - 1;
      JFT_Iter *i = iter->sub.many.iters + k;
      // as long as the position is out of bounds on the iter, reload it
      // if the iter is exhausted, remove it from active and skip to next
      while (i->batch.position >= i->batch.size)
        if (!JFT_iter_next(i)) {
          iter->sub.many.active &= ~(1LLU << k);
          goto nextActive;
        }
      if ((elem = i->batch.data[i->batch.position]) == min) {
        hasMin |= (1LLU << k);
      } else if (elem < min) {
        min = elem;
        hasMin = (1LLU << k);
      }
    nextActive:
      /* done with the active iter, move on to next */;
    }
    // if we can't find another min, all iters are exhausted and we are done
    // add the min to the batch iff everyone has it (main difference b/w any & all)
    if (!hasMin)
      break;
    else if (hasMin == iter->sub.many.exists)
      words[iter->batch.size++] = min;
    // advance any iters we found the min on
    for (int b = ffsll(hasMin); b; b = ffsll(hasMin &= hasMin - 1))
      iter->sub.many.iters[b - 1].batch.position++;
  }
  return True;
}

JFT_Boolean JFT_iter_but_next(JFT_Iter *iter) {
  // a - b - c - d - ...
  // if the first iter is exhausted, we are done
  if (ffsll(iter->sub.many.exists) != ffsll(iter->sub.many.active))
    return False;

  JFT_Word *words = iter->batch.data = iter->words;
  for (iter->batch.size = 0; iter->batch.size < JFT_ITER_WORDS; ) {
    // scour the active iters for the next min element, until we fill a batch
    JFT_Mask hasMin = 0;
    JFT_Word min = -1, elem;
    JFT_Mask active = iter->sub.many.active;
    for (int b = ffsll(active); b; b = ffsll(active &= active - 1)) {
      JFT_Amount k = b - 1;
      JFT_Iter *i = iter->sub.many.iters + k;
      // as long as the position is out of bounds on the iter, reload it
      // if the iter is exhausted, remove it from active and skip to next
      while (i->batch.position >= i->batch.size)
        if (!JFT_iter_next(i)) {
          iter->sub.many.active &= ~(1LLU << k);
          goto nextActive;
        }
      if ((elem = i->batch.data[i->batch.position]) == min) {
        hasMin |= (1LLU << k);
      } else if (elem < min) {
        min = elem;
        hasMin = (1LLU << k);
      }
    nextActive:
      /* done with the active iter, move on to next */;
    }
    // if we can't find another min, all iters are exhausted and we are done
    // add the min to the batch iff only the first has it (main difference b/w but & all)
    if (!hasMin)
      break;
    else if (hasMin == (1LLU << (ffsll(iter->sub.many.exists) - 1)))
      words[iter->batch.size++] = min;
    // advance any iters we found the min on
    for (int b = ffsll(hasMin); b; b = ffsll(hasMin &= hasMin - 1))
      iter->sub.many.iters[b - 1].batch.position++;
  }
  return True;
}

/* Pure memory buffers */

int JFT_membuf_ensure(JFT_Buffer *buf, size_t more) {
  size_t minSize = buf->mark + more;
  if (buf->size < minSize) {
    size_t newSize = buf->factor * minSize;
    uint8_t *data = realloc(buf->data, newSize);
    if (data == NULL)
      return ENoMem;
    buf->size = newSize;
    buf->data = data;
  }
  return Ok;
}

JFT_Buffer *JFT_membuf() {
  JFT_Buffer *buf = calloc(1, sizeof(JFT_Buffer));
  if (buf == NULL)
    return NULL;
  buf->factor = 2;
  buf->ensure = JFT_membuf_ensure;
  return buf;
}

JFT_Buffer *JFT_membuf_free(JFT_Buffer *buf) {
  free(buf->data);
  free(buf);
  return NULL;
}
