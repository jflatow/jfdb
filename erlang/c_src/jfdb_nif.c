#include <string.h>
#include "erl_nif.h"
#include "jfdb.h"

/* Common Erlang Terms */

#define ATOM(Val)         (enif_make_atom(env, Val))
#define BIN(Bin)          (enif_make_binary(env, &Bin))
#define CONS(H, T)        (enif_make_list_cell(env, H, T))
#define PAIR(A, B)        (enif_make_tuple2(env, A, B))
#define STRING(Val)       (enif_make_string(env, Val, ERL_NIF_LATIN1))
#define TERM_EQ(lhs, rhs) (enif_compare(lhs, rhs) == 0)
#define JFDB(jfdb)        (PAIR(ATOM_JFDB, make_reference(env, jfdb)))
#define ASYNC(R)          (PAIR(ATOM_JFDB, R))
#define ERROR(R)          (PAIR(ATOM_ERROR, R))
#define ERROR_JFDB(jfdb)  (ERROR(STRING(JFDB_str_error(jfdb->db))))

// NB: we would create these statically in a process independent env
//     however that doesn't work: is alloc env allowed in nif load?

#define ATOM_JFDB          ATOM("jfdb")
#define ATOM_OK            ATOM("ok")
#define ATOM_UNDEFINED     ATOM("undefined")
#define ATOM_TEMPORARY     ATOM("temporary")
#define ATOM_BADARG        ATOM("badarg")
#define ATOM_EALLOC        ATOM("ealloc")
#define ATOM_ERROR         ATOM("error")
#define ATOM_FETCH         ATOM("fetch")
#define ATOM_ANNUL         ATOM("annul")
#define ATOM_STORE         ATOM("store")
#define ATOM_QUERY         ATOM("query")
#define ATOM_LIMIT         ATOM("limit")
#define ATOM_FLUSH         ATOM("flush")
#define ATOM_CRUSH         ATOM("crush")
#define ATOM_CLOSE         ATOM("close")
#define ATOM_PRIMARY       ATOM("primary")
#define ATOM_INDICES       ATOM("indices")
#define ATOM_KEYS          ATOM("keys")
#define ATOM_VALS          ATOM("vals")
#define ATOM_ANY           ATOM("any")
#define ATOM_ALL           ATOM("all")
#define ATOM_BUT           ATOM("but")

#define ERROR_BADARG	   ERROR(ATOM_BADARG)
#define ERROR_EALLOC	   ERROR(ATOM_EALLOC)

/* Definitions */

typedef struct queue queue;
typedef struct message message;
typedef struct ErlJFDB ErlJFDB;
typedef struct ErlJFDBSub ErlJFDBSub;
typedef ERL_NIF_TERM (*ErlJFDBFn)(ErlJFDB *, message *);

struct ErlJFDB {
  ErlNifTid tid;
  JFDB *db;
  queue *msgs;
  uint8_t kdata[2][JFT_KEY_LIMIT]; // NB: trade fixed cost for less malloc
};

struct ErlJFDBSub {
  ErlJFDB *parent;
  JFT_Stem prefix;
};

struct message {
  message *next;
  ErlNifEnv *env;
  ErlNifPid from;
  ErlJFDBFn func;
  ErlJFDBSub *subj;
  ERL_NIF_TERM term;
};

struct queue {
  ErlNifMutex *lock;
  ErlNifCond *cond;
  message *head;
  message *tail;
  long length;
};

static ErlNifResourceType *ErlJFDBType;
static ErlNifResourceType *ErlJFDBSubType;
static message STOP = {};

/* Messages & Queue */

static void
message_free(message *m) {
  if (m->env)
    enif_free_env(m->env);
  enif_free(m);
}

static message *
message_new(ErlNifEnv *env, ErlJFDBFn func, ErlJFDBSub *subj, ERL_NIF_TERM term) {
  message *m;
  if (!(m = (message *)enif_alloc(sizeof(message))))
    return NULL;

  if (!(m->env = enif_alloc_env())) {
    message_free(m);
    return NULL;
  }

  if (env)
    enif_self(env, &m->from);

  m->next = NULL;
  m->func = func;
  m->subj = subj;
  m->term = term ? enif_make_copy(m->env, term) : 0;
  return m;
}

static queue *
queue_new() {
  queue *q;
  if ((q = (queue *)enif_alloc(sizeof(queue))) == NULL)
    goto error;
  if ((q = memset(q, 0, sizeof(queue))) == NULL)
    goto error;
  if ((q->lock = enif_mutex_create("queue_lock")) == NULL)
    goto error;
  if ((q->cond = enif_cond_create("queue_cond")) == NULL)
    goto error;
  return q;

 error:
  if (q->lock)
    enif_mutex_destroy(q->lock);
  if (q->cond)
    enif_cond_destroy(q->cond);
  if (q)
    enif_free(q);
  return NULL;
}

static void
queue_free(queue *q) {
  enif_cond_destroy(q->cond);
  enif_mutex_destroy(q->lock);
  enif_free(q);
}

static void
queue_push(queue *q, message *m) {
  enif_mutex_lock(q->lock);

  if (q->tail != NULL)
    q->tail->next = m;

  q->tail = m;

  if (q->head == NULL)
    q->head = q->tail;

  q->length++;

  enif_cond_signal(q->cond);
  enif_mutex_unlock(q->lock);
}

static message *
queue_pop(queue *q) {
  message *m;
  enif_mutex_lock(q->lock);

  while (q->head == NULL)
    enif_cond_wait(q->cond, q->lock);

  m = q->head;
  q->head = m->next;

  if (q->head == NULL)
    q->tail = NULL;

  q->length--;

  enif_mutex_unlock(q->lock);
  return m;
}

/* Support */

static ERL_NIF_TERM
make_reference(ErlNifEnv *env, void *res) {
  ERL_NIF_TERM ref = enif_make_resource(env, res);
  enif_release_resource(res);
  return ref;
}

static uint64_t
list_length_max(ErlNifEnv *env, const ERL_NIF_TERM list, uint64_t max) {
  uint64_t length = 0;
  ERL_NIF_TERM head, tail = list;
  while (length < max && enif_get_list_cell(env, tail, &head, &tail))
    length++;
  return length;
}

static uint64_t
keys_count_max(JFT_Keys *keys, uint64_t max) {
  uint64_t count = 0;
  while (count < max && JFT_keys_next(keys))
    count++;
  return count;
}

static JFT_Stem
affix_primary(ErlJFDBSub *subj, ErlNifBinary *bin) {
  if (subj) {
    memcpy(subj->prefix.data + subj->prefix.size, bin->data, bin->size);
    return (JFT_Stem) {
      .pre = JFT_SYMBOL_PRIMARY,
      .size = subj->prefix.size + bin->size + 1,
      .data = subj->prefix.data
    };
  } else {
    return (JFT_Stem) {
      .pre = JFT_SYMBOL_PRIMARY,
      .size = bin->size + 1,
      .data = bin->data
    };
  }
}

/* JFDB */

static int
ErlJFDB_open_flags(ErlNifEnv *env, const ERL_NIF_TERM opts) {
  int flags = 0;
  ERL_NIF_TERM head, tail = opts;
  if (!enif_is_list(env, opts))
    return -1;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    if (TERM_EQ(head, ATOM_TEMPORARY))
      flags |= JFDB_TEMPORARY;
    else
      return -1;
  }
  return flags;
}

static int
ErlJFDB_write_flags(ErlNifEnv *env, const ERL_NIF_TERM opts) {
  int flags = 0;
  ERL_NIF_TERM head, tail = opts;
  if (!enif_is_list(env, opts))
    return -1;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    if (TERM_EQ(head, ATOM_FLUSH))
      flags |= JFDB_FLUSH;
    else
      return -1;
  }
  return flags;
}

static void
ErlJFDB_free(ErlNifEnv *env, void *res) {
  ErlJFDB *jfdb = (ErlJFDB *)res;
  queue_push(jfdb->msgs, &STOP);
}

static void
ErlJFDBSub_free(ErlNifEnv *env, void *res) {
  ErlJFDBSub *subj = (ErlJFDBSub *)res;
  enif_release_resource(subj->parent);
  free(subj->prefix.data);
}

static void *
ErlJFDB_run(void *res) {
  ErlJFDB *jfdb = (ErlJFDB *)res;
  queue *q = jfdb->msgs;

  for (message *m = queue_pop(q); m->func; m = queue_pop(q)) {
    enif_send(NULL, &m->from, m->env, m->func(jfdb, m));
    message_free(m);
  }

  JFDB_close(jfdb->db);
  queue_free(q);

  return NULL;
}

static ErlJFDB *
ErlJFDB_start(ErlNifEnv *env) {
  ErlJFDB *jfdb;
  if (!(jfdb = enif_alloc_resource(ErlJFDBType, sizeof(ErlJFDB))))
    goto error;
  if (!(jfdb = memset(jfdb, 0, sizeof(ErlJFDB))))
    goto error;
  if (!(jfdb->msgs = queue_new()))
    goto error;
  if (enif_thread_create("jfdb", &jfdb->tid, &ErlJFDB_run, jfdb, NULL))
    goto error;
  return jfdb;

 error:
  if (jfdb)
    enif_release_resource(jfdb);
  return NULL;
}

static ErlJFDBSub *
ErlJFDBSub_new(ErlNifEnv *env, ErlJFDB *parent, JFT_Stem *prefix) {
  ErlJFDBSub *subj;
  if (!(subj = enif_alloc_resource(ErlJFDBSubType, sizeof(ErlJFDBSub))))
    goto error;
  if (!(subj->prefix.data = malloc(JFT_KEY_LIMIT)))
    goto error;

  subj->parent = parent;
  enif_keep_resource((void *)parent);

  subj->prefix.size = prefix->size;
  memcpy(subj->prefix.data, prefix->data, prefix->size);

  return subj;

 error:
  if (subj)
    enif_release_resource(subj);
  return NULL;
}

/* Keys / fetch object support */

typedef struct {
  ErlJFDB *jfdb;
  ErlNifEnv *env;
  ErlNifBinary kbin, vbin;
  ERL_NIF_TERM list;
  JFT_Stem val;
  int flags;
} KVs;

static JFT_Status
kv_fold(JFT_Cursor *cursor, JFDB_Slice *slice, JFT_Boolean isTerminal) {
  KVs *acc = (KVs *)slice->acc;
  ErlNifEnv *env = acc->env;
  int vals = acc->flags & JFDB_VALS;

  if (isTerminal) {
    if (JFDB_get_value(slice->db, cursor->node, &acc->val)) {
      if (!(enif_alloc_binary(slice->stem->size - slice->zero, &acc->kbin)))
        return ENoMem;
      memcpy(acc->kbin.data, slice->stem->data + slice->zero - 1, acc->kbin.size);

      if (vals) {
        if (!(enif_alloc_binary(acc->val.size, &acc->vbin))) {
          enif_release_binary(&acc->kbin);
          return ENoMem;
        }
        memcpy(acc->vbin.data, acc->val.data, acc->vbin.size);
        acc->list = CONS(PAIR(BIN(acc->kbin), BIN(acc->vbin)), acc->list);
      } else {
        acc->list = CONS(BIN(acc->kbin), acc->list);
      }
    }
  } else if (vals) {
    ErlJFDBSub *subj;
    if (!(subj = ErlJFDBSub_new(env, acc->jfdb, slice->stem)))
      return ENoMem;
    if (!(enif_alloc_binary(slice->stem->size - slice->zero, &acc->kbin)))
      return ENoMem;
    memcpy(acc->kbin.data, slice->stem->data + slice->zero - 1, acc->kbin.size);
    acc->list = CONS(PAIR(BIN(acc->kbin), JFDB(subj)), acc->list);
  }
  return Next;
}

static ERL_NIF_TERM
fold_kvs(ErlJFDB *jfdb, ErlNifEnv *env, JFT_Stem prefix, JFT_Symbol *stop, int flags) {
  KVs acc = (KVs) {
    .jfdb = jfdb,
    .env = env,
    .list = enif_make_list(env, 0),
    .flags = flags
  };
  // NB: make sure the prefix data buffer is safe to write to
  prefix.data = memcpy(jfdb->db->keyData, prefix.data, prefix.size - !!prefix.pre);
  if (JFDB_fold(jfdb->db, &prefix, stop, &kv_fold, &acc, JFT_FLAGS_REVERSE) != Ok)
    return ERROR_EALLOC;
  return acc.list;
}


static ERL_NIF_TERM
ErlJFDB_keys_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  ErlNifBinary bin;
  int arity;
  const ERL_NIF_TERM *args;
  if (!enif_get_tuple(env, msg->term, &arity, &args) || arity != 2)
    return ASYNC(ERROR_BADARG);
  if (!enif_inspect_iolist_as_binary(env, args[0], &bin))
    return ASYNC(ERROR_BADARG);
  if (!enif_is_list(env, args[1]))
    return ASYNC(ERROR_BADARG);

  JFT_Stem key = (JFT_Stem) {
    .size = bin.size,
    .data = bin.data
  };
  ERL_NIF_TERM head, tail = args[1];
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    if (TERM_EQ(head, ATOM_PRIMARY)) {
      key.pre = JFT_SYMBOL_PRIMARY;
      key.size += 1;
    } else if (TERM_EQ(head, ATOM_INDICES)) {
      key.pre = JFT_SYMBOL_INDICES;
      key.size += 1;
    } else {
      return ASYNC(ERROR_BADARG);
    }
  }
  if (!key.pre)
    key = affix_primary(msg->subj, &bin);

  return ASYNC(fold_kvs(jfdb, env, key, NULL, JFDB_KEYS));
}

static ERL_NIF_TERM
ErlJFDB_fetch_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  ErlNifBinary kbin, vbin;
  if (!enif_inspect_iolist_as_binary(env, msg->term, &kbin))
    return ASYNC(ERROR_BADARG);

  JFT_Symbol stop = 0;
  JFT_Cursor cursor;
  JFT_Stem val, key = affix_primary(msg->subj, &kbin);
  if (JFDB_find(jfdb->db, &cursor, &key)) {
    if (JFT_cursor_at_terminal(&cursor)) {
      // terminal: return the value or undefined
      if (JFDB_get_value(jfdb->db, cursor.node, &val)) {
        if (!(enif_alloc_binary(val.size, &vbin)))
          return ASYNC(ERROR_EALLOC);
        memcpy(vbin.data, val.data, vbin.size);
        return ASYNC(BIN(vbin));
      }
      return ASYNC(ATOM_UNDEFINED);
    } else if (cursor.symbol == JFT_SYMBOL_ANY) {
      // post-terminal: return undefined
      return ASYNC(ATOM_UNDEFINED);
    } else {
      // non-terminal (container): return list of kv
      return ASYNC(fold_kvs(jfdb, env, key, &stop, JFDB_KEYS | JFDB_VALS));
    }
  }
  return ASYNC(ATOM_UNDEFINED);
}

static ERL_NIF_TERM
ErlJFDB_annul_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  ErlNifBinary bin;
  int arity, flags;
  const ERL_NIF_TERM *args;
  if (!enif_get_tuple(env, msg->term, &arity, &args) || arity != 2)
    return ASYNC(ERROR_BADARG);
  if (!enif_inspect_iolist_as_binary(env, args[0], &bin))
    return ASYNC(ERROR_BADARG);
  if ((flags = ErlJFDB_write_flags(env, args[1])) < 0)
    return ASYNC(ERROR_BADARG);

  JFT_Stem key = affix_primary(msg->subj, &bin);
  if (JFDB_has_error(JFDB_annul(jfdb->db, &key, flags)))
    return ASYNC(ERROR_JFDB(jfdb));
  return ASYNC(ATOM_OK);
}

static ERL_NIF_TERM
ErlJFDB_store_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  ErlNifBinary kbin, vbin;
  int arity, flags;
  unsigned numIndices;
  const ERL_NIF_TERM *args;
  if (!enif_get_tuple(env, msg->term, &arity, &args) || arity != 4)
    return ASYNC(ERROR_BADARG);
  if (!enif_inspect_iolist_as_binary(env, args[0], &kbin))
    return ASYNC(ERROR_BADARG);
  if (!enif_inspect_iolist_as_binary(env, args[1], &vbin))
    return ASYNC(ERROR_BADARG);
  if (!enif_get_list_length(env, args[2], &numIndices))
    return ASYNC(ERROR_BADARG);
  if ((flags = ErlJFDB_write_flags(env, args[3])) < 0)
    return ASYNC(ERROR_BADARG);

  JFT_Stem key = affix_primary(msg->subj, &kbin);
  JFT_Stem val = (JFT_Stem) {
    .size = vbin.size,
    .data = vbin.data
  };

  ERL_NIF_TERM head, tail = args[2];
  JFT_Stem indices[numIndices];
  for (int i = 0; enif_get_list_cell(env, tail, &head, &tail); i++) {
    if (!enif_inspect_iolist_as_binary(env, head, &kbin))
      return ASYNC(ERROR_BADARG);
    indices[i] = (JFT_Stem) {
      .pre = JFT_SYMBOL_INDICES,
      .size = kbin.size + 1,
      .data = kbin.data
    };
  }

  if (JFDB_has_error(JFDB_store(jfdb->db, &key, &val, indices, numIndices, flags)))
    return ASYNC(ERROR_JFDB(jfdb));
  return ASYNC(ATOM_OK);
}

static int
build_key_iter(ErlJFDB *jfdb, JFT_Iter *iter, JFT *trie, ErlNifEnv *env, JFT_Stem *key) {
  JFT_Cursor cursor = JFT_cursor(trie);
  if (JFT_cursor_find(&cursor, key) < JFT_SYMBOL_NIL) {
    if (JFT_cursor_at_terminal(&cursor)) {
      // terminal: produce a simple leaf iter
      *iter = JFT_iter_leaf(JFT_leaf(cursor.node));
      return 0;
    } else {
      // non-terminal: produce a [recursive] any(leaf) iter
      JFT_Iter *iters = NULL;
      JFT_Stem k1 = JFT_key_copy(key, jfdb->kdata[0]);
      JFT_Stem k2 = JFT_key_copy(key, jfdb->kdata[1]);
      JFT_Cursor c1 = cursor;
      JFT_Cursor c2 = cursor;
      JFT_Keys keys = (JFT_Keys) {
        .cursor = &c1,
        .stem = &k1,
        .zero = key->size,
        .direction = Forward
      };
      JFT_Keys probe = (JFT_Keys) {
        .cursor = &c2,
        .stem = &k2,
        .zero = key->size,
        .direction = Forward
      };
      // test the water with another key iter to count how many keys are left
      for (uint64_t N = 0; (N = keys_count_max(&probe, JFT_MASK_CAPACITY)); ) {
        // allocate iters in chunks of N, using the last one recursively
        iters = malloc(N * sizeof(JFT_Iter));
        *iter = JFT_iter_any(iters, JFT_MASK_ACTIVE(N));
        iter->owner = 1;
        for (int i = 0; i < N && i < JFT_MASK_CAPACITY - 1; i++) {
          JFT_keys_next(&keys);
          iters[i] = JFT_iter_leaf(JFT_leaf(c1.node));
        }
        if (N == JFT_MASK_CAPACITY)
          iter = &iters[N - 1];
        // back the probe to where we actually landed
        c2 = c1;
        k2 = JFT_key_copy(&k1, jfdb->kdata[1]);
      }
      if (iters)
        return 0;
    }
  }
  // not found or N == 0
  *iter = JFT_iter_none();
  return 0;
}

static int
build_iter(ErlJFDB *jfdb, JFT_Iter *iter, JFT *trie, ErlNifEnv *env, const ERL_NIF_TERM query) {
  // NB: there are a quite a few possibilities for optimization here
  //     e.g. to avoid recursion through the stack
  //     but we leave those as exercises for later if/when they are relevant
  int arity;
  const ERL_NIF_TERM *term;
  if (enif_get_tuple(env, query, &arity, &term) && arity == 2) {
    uint64_t N = list_length_max(env, term[1], JFT_MASK_CAPACITY);

    // we can't construct empty JFT 'many' iters, so just exit early with an empty iter
    if (N == 0) {
      *iter = JFT_iter_none();
      return 0;
    }

    // construct an iter directly for up to first capacity - 1 elements
    JFT_Iter *iters = malloc(N * sizeof(JFT_Iter));
    ERL_NIF_TERM head, tail = term[1];
    for (int i = 0; i < N && i < JFT_MASK_CAPACITY - 1; i++) {
      enif_get_list_cell(env, tail, &head, &tail);
      build_iter(jfdb, &iters[i], trie, env, head);
    }
    if (N == JFT_MASK_CAPACITY) {
      // if we are at capacity, the last iter recursively operates on the rest
      if (TERM_EQ(term[0], ATOM_ALL))
        build_iter(jfdb, &iters[JFT_MASK_CAPACITY - 1], trie, env, PAIR(ATOM_ALL, head));
      else
        build_iter(jfdb, &iters[JFT_MASK_CAPACITY - 1], trie, env, PAIR(ATOM_ANY, head));
    }

    if (TERM_EQ(term[0], ATOM_ALL))
      *iter = JFT_iter_all(iters, JFT_MASK_ACTIVE(N));
    else if (TERM_EQ(term[0], ATOM_BUT))
      *iter = JFT_iter_but(iters, JFT_MASK_ACTIVE(N));
    else
      *iter = JFT_iter_any(iters, JFT_MASK_ACTIVE(N));

    // indicate that this is a 'many' iter and we want to free it
    iter->owner = 1;

    return 0;
  }

  ErlNifBinary kbin;
  if (enif_inspect_iolist_as_binary(env, query, &kbin)) {
    JFT_Stem key = (JFT_Stem) {
      .pre = JFT_SYMBOL_INDICES,
      .size = kbin.size + 1,
      .data = kbin.data
    };
    return build_key_iter(jfdb, iter, trie, env, &key);
  }
  return -1;
}

static void
destroy_iter(ErlJFDB *jfdb, JFT_Iter *iter) {
  if (iter->owner) {
    JFT_Mask exists = iter->sub.many.exists;
    for (JFT_Amount b = ffsll(exists); b; b = ffsll(exists &= exists - 1))
      destroy_iter(jfdb, &iter->sub.many.iters[b - 1]);
    free(iter->sub.many.iters);
  }
}

static ERL_NIF_TERM
ErlJFDB_query_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  int arity, flags = 0;
  const ERL_NIF_TERM *args, *opt;
  if (!enif_get_tuple(env, msg->term, &arity, &args) || arity != 2)
    return ASYNC(ERROR_BADARG);
  if (!enif_is_list(env, args[1]))
    return ASYNC(ERROR_BADARG);

  unsigned long limit = -1;
  ERL_NIF_TERM head, tail = args[1];
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    if (enif_get_tuple(env, head, &arity, &opt) && arity == 2 && TERM_EQ(opt[0], ATOM_LIMIT))
      enif_get_ulong(env, opt[1], &limit); // NB: ignore if not NaN -> gives default behavior
    else if (TERM_EQ(head, ATOM_KEYS))
      flags |= JFDB_KEYS;
    else if (TERM_EQ(head, ATOM_VALS))
      flags |= JFDB_VALS;
    else
      return ASYNC(ERROR_BADARG);
  }

  // perform search trie by trie
  // NB:
  //  if an index key is changed from terminal <-> non-terminal, behavior is undefined
  //  we call this 'changing specificity', since the key becomes more or less specific
  //  actually it's well defined, but we reserve the right to change behavior:
  //   - once DB is compacted, the latest specificity will clobber
  //     at that point, old values for the clobbered indices will get lost
  //   - however, before DB is compacted, we return results for all matching specificities
  //     we could prevent this, but it's not worth the complexity / cost
  //     its also not necessarily more or less correct than what we are doing
  //   - for now, changing key generality is considered undefined behavior
  //     assuming we don't change our mind, the advice for handling this case will be:
  //      if you change index key specificity, just make sure to crush right afterwards
  ERL_NIF_TERM results = enif_make_list(env, 0), k, v;
  ErlNifBinary kbin, vbin;
  JFDB *db = jfdb->db;
  JFT *trie;
  JFT_Iter iter;
  JFT_Stem key, val;
  JFT_Count n = 0;
  for (JFT_Offset pos = db->tip.cp.offset; pos; pos = JFT_parent_offset(trie)) {
    trie = JFDB_get_trie(db, pos);
    if (build_iter(jfdb, &iter, trie, env, args[0]) < 0) {
      destroy_iter(jfdb, &iter);
      return ASYNC(ERROR_BADARG);
    }

    do {
      for (JFT_Count i = 0; i < iter.batch.size && n < limit; i++, n++) {
        JFT *node = trie + iter.batch.data[i];
        if (JFT_is_dirty(node))
          continue;

        // always get the value (doesn't really cost us anything until we copy)
        JFDB_get_value(db, node, &val);

        // produce the right {k, v} depending on whether we are dereferencing
        if (flags & JFDB_KEYS) {
          key = JFT_key(node, db->keyData);
          if (!(enif_alloc_binary(key.size - 1, &kbin))) {
            destroy_iter(jfdb, &iter);
            return ASYNC(ERROR_EALLOC);
          }
          memcpy(kbin.data, key.data, kbin.size);
          k = BIN(kbin);
        } else {
          k = enif_make_int64(env, (int64_t)node);
        }

        if (flags & JFDB_VALS) {
          if (!(enif_alloc_binary(val.size, &vbin))) {
            destroy_iter(jfdb, &iter);
            return ASYNC(ERROR_EALLOC);
          }
          memcpy(vbin.data, val.data, vbin.size);
          v = BIN(vbin);
        } else {
          v = enif_make_int64(env, val.size);
        }

        // create the tuple and add it to the list
        results = CONS(PAIR(k, v), results);
      }
    } while (JFT_iter_next(&iter) && n < limit);
    destroy_iter(jfdb, &iter);
  }

  return ASYNC(results);
}

static ERL_NIF_TERM
ErlJFDB_flush_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  if (JFDB_has_error(JFDB_flush(jfdb->db)))
    return ASYNC(ERROR_JFDB(jfdb));
  return ASYNC(ATOM_OK);
}

static ERL_NIF_TERM
ErlJFDB_crush_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  if (JFDB_has_error(JFDB_crush(jfdb->db)))
    return ASYNC(ERROR_JFDB(jfdb));
  return ASYNC(ATOM_OK);
}

static ERL_NIF_TERM
ErlJFDB_close_async(ErlJFDB *jfdb, message *msg) {
  ErlNifEnv *env = msg->env;
  if (jfdb->db)
    if ((jfdb->db = JFDB_close(jfdb->db)))
        return ASYNC(ERROR_JFDB(jfdb));
  return ASYNC(ATOM_OK);
}

static ERL_NIF_TERM
ErlJFDB_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
  ErlJFDB *jfdb;
  if (!(jfdb = ErlJFDB_start(env)))
    return ERROR_EALLOC;

  unsigned size;
  if (enif_get_list_length(env, argv[0], &size)) {
    char path[size + 1];
    int flags;
    if (enif_get_string(env, argv[0], path, size + 1, ERL_NIF_LATIN1) != size + 1)
      return ERROR_BADARG;
    if ((flags = ErlJFDB_open_flags(env, argv[1])) < 0)
    return ASYNC(ERROR_BADARG);
    if (!(jfdb->db = JFDB_open(path, flags)))
      return ERROR_EALLOC;
    if (JFDB_has_error(jfdb->db))
      return ERROR_JFDB(jfdb);
    return JFDB(jfdb);
  }

  return ERROR_BADARG;
}

static ERL_NIF_TERM
ErlJFDB_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[]) {
  ErlJFDB *jfdb = NULL;
  ErlJFDBSub *subj = NULL;
  int arity;
  const ERL_NIF_TERM *args;
  if (!enif_get_tuple(env, argv[0], &arity, &args) || arity != 2)
    return ERROR_BADARG;
  if (!enif_get_resource(env, args[1], ErlJFDBType, (void **)&jfdb)) {
    if (!enif_get_resource(env, args[1], ErlJFDBSubType, (void **)&subj))
      return ERROR_BADARG;
    jfdb = subj->parent;
  }

  // XXX: use direct calls instead of additional dispatch?
  message *msg;
  if (TERM_EQ(argv[1], ATOM_KEYS))
    msg = message_new(env, &ErlJFDB_keys_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_FETCH))
    msg = message_new(env, &ErlJFDB_fetch_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_ANNUL))
    msg = message_new(env, &ErlJFDB_annul_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_STORE))
    msg = message_new(env, &ErlJFDB_store_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_QUERY))
    msg = message_new(env, &ErlJFDB_query_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_FLUSH))
    msg = message_new(env, &ErlJFDB_flush_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_CRUSH))
    msg = message_new(env, &ErlJFDB_crush_async, subj, argv[2]);
  else if (TERM_EQ(argv[1], ATOM_CLOSE))
    msg = message_new(env, &ErlJFDB_close_async, subj, argv[2]);
  else
    return ERROR_BADARG;

  if (msg == NULL)
    return ERROR_EALLOC;

  queue_push(jfdb->msgs, msg);
  return argv[0];
}

/* NIF Initialization */

static ErlNifFunc nif_funcs[] =
  {
    {"open", 2, ErlJFDB_open},
    {"call", 3, ErlJFDB_call}
  };

static int
on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
  ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
  ErlJFDBType = enif_open_resource_type(env, NULL, "jfdb", &ErlJFDB_free, flags, NULL);
  if (ErlJFDBType == NULL)
    return -1;
  ErlJFDBSubType = enif_open_resource_type(env, NULL, "jfdb-sub", &ErlJFDBSub_free, flags, NULL);
  if (ErlJFDBSubType == NULL)
    return -1;
  return 0;
}

static int
on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static void
on_unload(ErlNifEnv* env, void* priv_data) {}

ERL_NIF_INIT(jfdb_nif, nif_funcs, &on_load, NULL, &on_upgrade, &on_unload);