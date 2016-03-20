#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "db.h"

#define NBLOCKS(size) (RUP(size, JFDB_BLOCK_SIZE))

int JFDB_file_open(JFDB *db, const char *path) {
  // get the fd for keys & vals, and sanity check that they look like valid files
  // vals has a different header, but the magic / version are the same
  int fd;
  if ((fd = open(path, O_RDWR, 0644)) < 0) {
    if (errno == ENOENT) {
      if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644)) < 0) {
        JFDB_set_error(db, JFDB_EFILE, "could not create ('%.32s')", path);
      } else {
        // new file: write the header
        db->tip.magic = JFDB_MAGIC;
        db->tip.version = JFDB_VERSION;
        if (write(fd, &db->tip, sizeof(db->tip)) < sizeof(db->tip))
          JFDB_set_error(db, JFDB_EFILE, "failed to write ('%.32s')", path);
      }
    } else {
      JFDB_set_error(db, JFDB_EFILE, "cannot open file ('%.32s')", path);
    }
  } else {
    // existing file: read the header
    if (read(fd, &db->tip, sizeof(db->tip)) < sizeof(db->tip))
      JFDB_set_error(db, JFDB_EFILE, "failed to read ('%.32s')", path);
    else if (db->tip.magic != JFDB_MAGIC && db->tip.version != JFDB_VERSION)
      JFDB_set_error(db, JFDB_EFILE, "invalid file ('%.32s')", path);
  }
  return fd;
}

int JFDB_mmap_file(JFDB *db, int kv, size_t cover, int factor) {
  // if the mmap already includes cover, do nothing
  // otherwise increase the mapping by a size of factor
  JFDB_MMap *m = kv == JFDB_KEYS ? &db->kmap : &db->vmap;
  if (m->size >= cover)
    return 0;

  // we need to remap the memory, there are some open questions and TODOs:
  //  - if we munmap before mmapping again:
  //     we temporarily release the references to the underlying memory
  //     potentially allowing us to re-use the address range
  //     potentially avoiding TLB misses if the OS hasn't released the memory
  //     there's a lot of potential but it could just as easily work against us
  //  - if we mmap again before munmapping:
  //     we ensure that we always retain a reference to the memory
  //     however we are guaranteed to get a different address range
  //  - XXX: we should be taking advantage of mremap when it's available
  //  - alternatively, we could impose an upper bound on the file size and map it once
  //    but its not polite (at best) for a library to hog the address space like that
  //  - we map nearest multiple of PAGESIZE, for which we will anyway incur the cost
  int prot = kv == JFDB_KEYS ? PROT_READ | PROT_WRITE : PROT_READ;
  if (m->size)
    munmap(m->map, m->size);
  m->size = RTM(cover * factor, sysconf(_SC_PAGESIZE));
  m->map = mmap(NULL, m->size, prot, MAP_SHARED, m->fd, 0);
  if (m->map == MAP_FAILED)
    JFDB_set_error(db, JFDB_EMMAP, "cannot mmap %s", kv == JFDB_KEYS ? "keys" : "vals");
  return 1;
}

void JFDB_mmap_free(JFDB *db, int kv) {
  JFDB_MMap *m = kv == JFDB_KEYS ? &db->kmap : &db->vmap;
  if (m->map != MAP_FAILED)
    munmap(m->map, m->size);
  if (m->fd >= 0)
    close(m->fd);
  m->size = 0;
}

static JFDB_Region JFDB_valloc(JFDB *db, size_t size) {
  JFDB_CheckPoint *cp = &db->tip.cp;
  uint32_t block;
  size_t blockSize = RTM(size, JFDB_BLOCK_SIZE);
  for (int i = 0; i < JFDB_NUM_GAPS; i++) {
    JFDB_Region *gap = &cp->gaps[i];
    if (blockSize <= gap->size) {
      // we found a big enough gap to hold the value
      // adjust the gap and return a block beginning where it was
      block = gap->block;
      gap->block += blockSize / JFDB_BLOCK_SIZE;
      gap->size -= blockSize;
      return (JFDB_Region) {
        .block = block,
        .size = size
      };
    }
  }

  // otherwise append to next block at the end of file
  // return a region no matter what, but db may have error
  block = NBLOCKS(cp->lengthVals);
  cp->lengthVals = block * JFDB_BLOCK_SIZE + size;
  if (ftruncate(db->vmap.fd, cp->lengthVals))
    JFDB_set_error(db, JFDB_EFILE, "failed to truncate vals");
  JFDB_mmap_file(db, JFDB_VALS, cp->lengthVals, 2);
  return (JFDB_Region) {
    .block = block,
    .size = size
  };
}

static void JFDB_add_to_free_list(JFDB *db, JFDB_Region region) {
  uint32_t lower = region.block;
  uint32_t upper = region.block + NBLOCKS(region.size);
  if (lower < db->freeList.lower)
    db->freeList.lower = lower;
  if (upper > db->freeList.upper)
    db->freeList.upper = upper;
  db->freeList.data[db->freeList.size++] = region;
}

static void JFDB_vfree(JFDB *db, JFDB_Region region) {
  if (db->freeList.size < JFDB_MAX_FREE - 1)
    JFDB_add_to_free_list(db, region);
  if (db->freeList.size >= JFDB_MAX_FREE)
    db->oughtFlush = True;
}

static size_t JFDB_voffset(JFDB_Region region) {
  return region.block * JFDB_BLOCK_SIZE;
}

JFDB *JFDB_open(const char *path, int flags) {
  JFDB *db = calloc(1, sizeof(JFDB));
  int len = strlen(path);
  if (db == NULL)
    return NULL;
  if (len == 0 || len > JFDB_MAX_PATH)
    return JFDB_set_error(db, JFDB_EPATH, "invalid path ('%.32s')", path);
  if ((db->path = strdup(path)) == NULL)
    return JFDB_set_error(db, JFDB_ENOMEM, "out of memory");
  if ((db->scratch[0] = JFT_membuf()) == NULL)
    return JFDB_set_error(db, JFDB_ENOMEM, "out of memory");
  if ((db->scratch[1] = JFT_membuf()) == NULL)
    return JFDB_set_error(db, JFDB_ENOMEM, "out of memory");

  char fp[JFDB_MAX_PATH + 6];
  db->flags = flags;

  // load vals before keys, because we want to keep the tip from keys
  snprintf(fp, sizeof(fp), "%s.vals", path);
  db->vmap.fd = JFDB_file_open(db, fp);
  if (JFDB_has_error(db))
    return db;

  snprintf(fp, sizeof(fp), "%s.keys", path);
  db->kmap.fd = JFDB_file_open(db, fp);
  if (JFDB_has_error(db))
    return db;

  // we'll need the *actual* size of the keys file to ensure correctness
  struct stat kstat;
  if (fstat(db->kmap.fd, &kstat))
    return JFDB_set_error(db, JFDB_EFILE, "failed to fstat keys");

  // load the checkpoint
  JFDB_CheckPoint *cp = &db->tip.cp;

  // map the files accordingly
  cp->lengthVals = MAX(cp->lengthVals, sizeof(JFDB_Header));
  JFDB_mmap_file(db, JFDB_VALS, cp->lengthVals, 1);
  if (JFDB_has_error(db))
    return db;

  cp->lengthKeys = MAX(cp->lengthKeys, sizeof(JFDB_Header));
  JFDB_mmap_file(db, JFDB_KEYS, cp->lengthKeys, 1);
  if (JFDB_has_error(db))
    return db;

  // ensure that the last checkpoint was written correctly, otherwise recompact
  if (kstat.st_size > cp->lengthKeys)
    return JFDB_compact(db, NULL);
  if (cp->offset && db->kmap.map[cp->lengthKeys - 1] != '\n')
    return JFDB_compact(db, NULL);
  return db;
}

JFDB *JFDB_close(JFDB *db) {
  // if for some strange reason we can't flush, don't free our resources yet
  if (db->isModified)
    if (JFDB_has_error(JFDB_flush(db)))
      return db;
  JFDB_mmap_free(db, JFDB_KEYS);
  JFDB_mmap_free(db, JFDB_VALS);
  JFT_membuf_free(db->scratch[0]);
  JFT_membuf_free(db->scratch[1]);
  free(db->path);
  free(db);
  return NULL;
}

JFDB *JFDB_mark_dirty(JFDB *db, const JFT *tx, JFT_Offset prior) {
  // look for collisions in the old keys
  // if we find any, dirty them, and deallocate the regions
  JFT_Cursor txCursor = JFT_cursor((JFT *)tx), otherCursor;
  JFT_Stem stem = (JFT_Stem) {
    .pre = JFT_SYMBOL_PRIMARY,
    .size = 1,
    .data = db->keyData
  };
  JFT_Keys keys = JFT_keys(&txCursor, &stem, Forward);
  while (JFT_keys_next(&keys)) {
    // walk the tries from prior, all the way to the base
    // it is crucial to start from the prior offset
    // so that we free regions even in tries that have become obsolete
    JFT *trie;
    for (JFT_Offset pos = prior; pos; pos = JFT_parent_offset(trie)) {
      trie = JFDB_get_trie(db, pos);
      JFT_cursor_init(&otherCursor, In, trie);
      // if we fall off the trie, we should continue the search backwards
      if (JFT_cursor_find(&otherCursor, keys.stem) == JFT_SYMBOL_NIL)
        continue;
      // otherwise, this trie overlaps the key: we need to clobber
      if (JFT_node_type(otherCursor.node) == Leaf) {
        // we can stop after we hit a leaf:
        //  a leaf must have already dirtied and freed its ancestors
        // if its already dirty, do nothing (so we don't double free)
        // NB: this can happen e.g. if two longer keys clobber a prefix
        if (!JFT_is_dirty(otherCursor.node)) {
          JFT_set_dirty((JFT *)otherCursor.node, True);
          JFT_Leaf leaf = JFT_leaf(otherCursor.node);
          if (leaf.size)
            JFDB_vfree(db, *(JFDB_Region *)leaf.data);
          break;
        }
      } else {
        // we have to dirty and free all leaves in any branches we clobber
        // and then we still need to continue searching ancestors
        // NB: scratch[0] is guaranteed to be be big enough for keys of tx
        //     we know because we already merged (at least) tx into it
        JFT_Stem subStem = (JFT_Stem) {.data = db->scratch[0]->data};
        JFT_Keys subKeys = JFT_keys(&otherCursor, &subStem, Forward);
        while (JFT_keys_next(&subKeys)) {
          if (!JFT_is_dirty(otherCursor.node)) {
            JFT_set_dirty((JFT *)otherCursor.node, True);
            JFT_Leaf leaf = JFT_leaf(otherCursor.node);
            if (leaf.size)
              JFDB_vfree(db, *(JFDB_Region *)JFT_leaf(otherCursor.node).data);
          }
        }
      }
    }
  }
  return db;
}

JFDB *JFDB_write(JFDB *db, const JFT *tx, int flags) {
  // bump the level, merge all the levels up to this one
  // if we are at or beyond the file level: compact everything into a new file
  JFDB_CheckPoint *cp = &db->tip.cp;
  if (++cp->levels >= (1LLU << db->tip.level))
    return JFDB_compact(db, tx);

  // the caller can force a flush using the flags
  if (flags & JFDB_FLUSH)
    db->oughtFlush = True;
  db->isModified = True;

  // walk back N levels from prior - merge tries into a new trie
  JFT_Offset prior = cp->offset;
  JFT *trie = JFDB_get_trie(db, prior);
  JFT_Amount N = ffsll(cp->levels) - 1;
  JFT_Cursor cursors[N + 1];
  cursors[0] = JFT_cursor((JFT *)tx);
  for (int i = 1; i <= N; i++) {
    cursors[i] = JFT_cursor(trie);
    trie = JFDB_get_trie(db, JFT_parent_offset(trie));
  }

  // make the new trie
  JFT_Buffer *buf = db->scratch[0];
  JFT *next = JFT_cursor_merge_new(cursors, N + 1, buf, 0);
  if (!next)
    return JFDB_set_error(db, JFDB_ETRIE, "failed to merge keys");

  // set the parent: if we merged than that of the last trie, otherwise the prior
  JFT_set_parent_offset(next, N ? JFT_parent_offset(cursors[N].root) : prior);

  // as soon as we change the file size, our last checkpoint becomes stale
  if (ftruncate(db->kmap.fd, cp->lengthKeys + buf->mark + 1))
    return JFDB_set_error(db, JFDB_EFILE, "failed to truncate keys");

  if (pwrite(db->kmap.fd, buf->data, buf->mark, cp->lengthKeys) < buf->mark)
    return JFDB_set_error(db, JFDB_EFILE, "failed to append to keys");
  if (pwrite(db->kmap.fd, "\n", 1, cp->lengthKeys + buf->mark) < 1)
    return JFDB_set_error(db, JFDB_EFILE, "failed to append keys separator");

  cp->offset = cp->lengthKeys;
  cp->lengthKeys += buf->mark + 1;

  // remap to cover the newly appended trie
  JFDB_mmap_file(db, JFDB_KEYS, cp->lengthKeys, 2);
  if (JFDB_has_error(db))
    return db;

  // now mark everything that was changed as dirty
  JFDB_mark_dirty(db, tx, prior);
  if (JFDB_has_error(db))
    return db;

  // flush if we need to flush
  if (db->oughtFlush)
    JFDB_flush(db);
  return db;
}

JFDB *JFDB_annul(JFDB *db, const JFT_Stem *primary, int flags) {
  JFT_Leaf leaf = (JFT_Leaf) {};
  JFT *tx = JFT_atom(primary, &leaf, NULL, 0, db->scratch[0], db->scratch[1]);
  if (!tx)
    return JFDB_set_error(db, JFDB_ETRIE, "failed to create atom");
  return JFDB_write(db, tx, flags);
}

JFDB *JFDB_store(JFDB *db,
                 const JFT_Stem *restrict primary,
                 const JFT_Stem *restrict value,
                 const JFT_Stem *restrict indices,
                 JFT_Count numIndices,
                 int flags) {
  JFDB_Region region = JFDB_valloc(db, value->size);
  if (JFDB_has_error(db))
    return db;
  if (pwrite(db->vmap.fd, value->data, value->size, JFDB_voffset(region)) < value->size)
    return JFDB_set_error(db, JFDB_EFILE, "failed to write value");

  JFT_Leaf leaf = (JFT_Leaf) {.size = 1, .data = (JFT_Word *)&region};
  JFT *tx = JFT_atom(primary, &leaf, indices, numIndices, db->scratch[0], db->scratch[1]);
  if (!tx)
    return JFDB_set_error(db, JFDB_ETRIE, "failed to create atom");
  return JFDB_write(db, tx, flags);
}

JFDB *JFDB_flush(JFDB *db) {
  // we only ever write a checkpoint if we are sure everything in it made it to disk
  // so make sure everything gets to disk, and then write a checkpoint
  // we can only reclaim space when we are sure that it is not being used anymore
  // now that we are flushing, it is safe to recompute the gap list we're using
  if (JFDB_fsync(db->vmap.fd))
    return JFDB_set_error(db, JFDB_EFILE, "failed to fsync vals");
  if (JFDB_fsync(db->kmap.fd))
    return JFDB_set_error(db, JFDB_EFILE, "failed to fsync keys");
  if (JFDB_msync(db->kmap.map, db->kmap.size, MS_SYNC))
    return JFDB_set_error(db, JFDB_EFILE, "failed to msync keys");

  // put the current gaps back in the running
  for (int i = 0; i < JFDB_NUM_GAPS; i++)
    JFDB_add_to_free_list(db, db->tip.cp.gaps[i]);

  // create a region map to cover the whole interval, default to not free
  JFDB_RegionMap *map = JFDB_region_map_alloc(db->freeList.lower, db->freeList.upper, 1);
  if (!map)
    return JFDB_set_error(db, JFDB_ENOMEM, "failed to create gap map");

  // add all the free regions
  for (int i = 0; i < db->freeList.size; i++)
    JFDB_region_map_clear(map, db->freeList.data[i]);

  // determine gaps
  JFDB_region_map_gaps(map, db->tip.cp.gaps);
  JFDB_region_map_free(map);

  db->freeList.size = 0;
  db->isModified = False;
  db->oughtFlush = False;

  if (pwrite(db->kmap.fd, &db->tip, sizeof(JFDB_Header), 0) < sizeof(JFDB_Header))
    return JFDB_set_error(db, JFDB_EFILE, "failed to write keys header");
  return db;
}

JFDB *JFDB_crush(JFDB *db) {
  // compact the DB (only) if there are multiple levels
  // NB: this doesn't guarantee nothing would change if we were to compact
  //     e.g. currently it may take multiple compactions to optimize branches after pruning
  if (db->tip.cp.levels == 0)
    return db;
  return JFDB_compact(db, NULL);
}

static JFDB *JFDB_calculate_gaps(JFDB *db, const JFT *trie) {
  // create a region map to cover all the vals
  uint32_t lower = NBLOCKS(sizeof(JFDB_Header));
  uint32_t upper = NBLOCKS(db->tip.cp.lengthVals);
  JFDB_RegionMap *map = JFDB_region_map_alloc(lower, upper, 0);
  if (!map)
    return JFDB_set_error(db, JFDB_ENOMEM, "failed to create region map");

  // add all the regions in the trie (values of primary keys)
  JFT_Cursor cursor = JFT_cursor((JFT *)trie);
  JFT_Stem stem = (JFT_Stem) {
    .pre = JFT_SYMBOL_PRIMARY,
    .size = 1,
    .data = db->keyData
  };
  JFT_Keys keys = JFT_keys(&cursor, &stem, Forward);
  for (int i = 0; JFT_keys_next(&keys); i++) {
    JFT_Leaf leaf = JFT_leaf(cursor.node);
    if (leaf.size)
      JFDB_region_map_store(map, *(JFDB_Region *)leaf.data);
  }

  // determine gaps
  JFDB_region_map_gaps(map, db->tip.cp.gaps);
  JFDB_region_map_free(map);

  return db;
}

JFDB *JFDB_compact(JFDB *db, const JFT *tx) {
  JFDB_CheckPoint *cp = &db->tip.cp;

  // merge all tries into a new trie - at this level there should be at most N
  // allow one additional trie (as argument), in case we are performing a write
  JFT_Amount N = db->tip.level, i = 0;
  JFT_Cursor cursors[N + 1];
  if (tx)
    cursors[i++] = JFT_cursor((JFT *)tx);

  // walk backwards from the latest checkpoint for as far as we can, one cursor per trie
  JFT *trie;
  for (JFT_Offset pos = db->tip.cp.offset; pos; pos = JFT_parent_offset(trie)) {
    trie = JFDB_get_trie(db, pos);
    cursors[i++] = JFT_cursor(trie);
  }

  // if there's nothing to merge (empty db), we are done
  if (i == 0)
    return db;

  JFT_Buffer *buf = db->scratch[0];
  JFT *next = JFT_cursor_merge_new(cursors, i, buf, 0);

  if (!next)
    return JFDB_set_error(db, JFDB_ETRIE, "failed to merge keys");

  JFDB_calculate_gaps(db, next);
  if (JFDB_has_error(db))
    return JFDB_set_error(db, JFDB_ETRIE, "failed to calculate gaps");

  // release the old file
  JFDB_mmap_free(db, JFDB_KEYS);

  // produce the new file
  char fp[JFDB_MAX_PATH + 6];
  char tp[JFDB_MAX_PATH + 10];
  snprintf(fp, sizeof(fp), "%s.keys", db->path);
  snprintf(tp, sizeof(tp), "%s.keys.tmp", db->path);
  db->kmap.fd = JFDB_file_open(db, tp);
  if (JFDB_has_error(db))
    return db;

  // write the new header: base the level on the # primary keys
  db->tip.level = flsll(((JFT_Root *)next)->numPrimary);
  cp->lengthKeys = sizeof(JFDB_Header) + buf->mark + 1;
  cp->levels = 0;
  cp->offset = sizeof(JFDB_Header);
  if (pwrite(db->kmap.fd, &db->tip, sizeof(JFDB_Header), 0) < sizeof(JFDB_Header))
    return JFDB_set_error(db, JFDB_EFILE, "failed to write compact keys header");

  // write the trie, sync to disk, rename the file
  if (pwrite(db->kmap.fd, buf->data, buf->mark, sizeof(JFDB_Header)) < buf->mark)
    return JFDB_set_error(db, JFDB_EFILE, "failed to write compact keys");
  if (pwrite(db->kmap.fd, "\n", 1, sizeof(JFDB_Header) + buf->mark) < 1)
    return JFDB_set_error(db, JFDB_EFILE, "failed to append keys separator");
  if (JFDB_fsync(db->kmap.fd))
    return JFDB_set_error(db, JFDB_EFILE, "failed to fsync compact keys");
  if (rename(tp, fp))
    return JFDB_set_error(db, JFDB_EFILE, "failed to rename compact keys");

  // map the new file
  JFDB_mmap_file(db, JFDB_KEYS, cp->lengthKeys, 1);
  if (JFDB_has_error(db))
    return db;

  // clear the between flush state
  db->freeList.size = 0;
  db->isModified = False;
  db->oughtFlush = False;

  // XXX: if we wanted to release some memory we could shrink buf (flag on open?)

  return db;
}

JFT *JFDB_get_trie(JFDB *db, JFT_Offset offset) {
  return (JFT *)((uint8_t *)db->kmap.map + offset);
}

JFT_Stem *JFDB_get_value(JFDB *db, JFT *node, JFT_Stem *value) {
  JFT_Leaf leaf = JFT_leaf(node);
  if (leaf.size == 0)
    return NULL;
  JFDB_Region region = *(JFDB_Region *)leaf.data;
  value->size = region.size;
  value->data = db->vmap.map + JFDB_voffset(region);
  return value;
}

JFT_Cursor *JFDB_find(JFDB *db, JFT_Cursor *cursor, const JFT_Stem *key) {
  JFT *trie;
  for (JFT_Offset pos = db->tip.cp.offset; pos; pos = JFT_parent_offset(trie)) {
    trie = JFDB_get_trie(db, pos);
    JFT_cursor_init(cursor, In, trie);
    if (JFT_cursor_find(cursor, key) != JFT_SYMBOL_NIL)
      return cursor;
  }
  return NULL;
}

static JFT_Status splice_fold(JFT_Cursor *cursors, JFT_MergeContext *ctx, JFT_Phase phase) {
  JFT_MergeFrame *frame = ctx->frame;
  JFDB_Slice *slice = (JFDB_Slice *)ctx->acc;
  JFT_Cursor *cursor = cursors + ffsll(frame->active) - 1;

  if (phase == Out) {
    if (slice->nth--)
      slice->stem->size--;
    return Ok;
  }

  if (cursor->symbol < JFT_SYMBOL_TOP)
    JFT_set_symbol_at_point(slice->stem, slice->stem->size, cursor->symbol);

  if (JFT_cursor_at_terminal(cursor)) {
    return slice->fun(cursor, slice, True);
  } else if (slice->stop && cursor->symbol == *slice->stop && slice->nth) {
    return slice->fun(cursor, slice, False);
  }
  if (slice->nth++)
    slice->stem->size++;
  return Step;
}

JFT_Status JFDB_fold(JFDB *db,
                     JFT_Stem *stem,
                     JFT_Symbol *stop,
                     JFDB_FoldFun fun,
                     void *restrict acc,
                     int flags) {
  // fold all active tries in the DB simultaneously, in order
  // as usual, stem serves as both input and output
  JFT_Amount N = db->tip.level, i = 0;
  JFT_Cursor cursors[N];
  JFT *trie;
  for (JFT_Offset pos = db->tip.cp.offset; pos; pos = JFT_parent_offset(trie)) {
    // we want all the cursors to be at the same position, so only add them if they have the prefix
    trie = JFDB_get_trie(db, pos);
    cursors[i] = JFT_cursor(trie);
    if (JFT_cursor_find(&cursors[i], stem) < JFT_SYMBOL_NIL)
      i++;
  }
  JFDB_Slice slice = (JFDB_Slice) {
    .db = db,
    .stem = stem,
    .stop = stop,
    .zero = stem->size,
    .fun = fun,
    .acc = acc
  };
  return JFT_cursor_merge(cursors, i, &splice_fold, &slice, flags);
}
