#include <gio/gio.h>
#include <glib-object.h>
#include <glib.h>
#include <nautilus-extension.h>

#define VD_BUILD_TAG                                                           \
  "VD_BUILD_TAG: COMPLETE-async-ffmpeg-helperpool-noplaceholder-2026-01-11"

#define COLUMN_ID "video-duration::duration"
#define ATTR_KEY "video-duration::duration"

/* Defaults */
#define DEFAULT_CACHE_MAX_ENTRIES 512
#define DEFAULT_QUEUE_MAX 256
#define DEFAULT_DEBOUNCE_MS 250
#define DEFAULT_NEGATIVE_TTL_SEC 600 /* 10 min */
#define DEFAULT_EVICT_RATIO_PERCENT 70
#define DEFAULT_DROP_NEGATIVE_TTL_MS 2000 /* drop 后短暂负缓存 */

#define DEFAULT_HELPER_MAX 2
#define DEFAULT_HELPER_REQS_BEFORE_RESTART 400
#define DEFAULT_HELPER_TIMEOUT_MS 2000
#define DEFAULT_HELPER_RETRY_TIMEOUT_MS 12000
#define DEFAULT_TIMEOUT_NEG_TTL_SEC 3

#ifndef VD_HELPER_PATH
#define VD_HELPER_PATH "/usr/lib/nautilus/extensions-4/vd-ffmpeg-helper"
#endif

/* Flags */
#define FLAG_INFLIGHT (1u << 0)
#define FLAG_HAS_VALUE (1u << 1)
#define FLAG_NEGATIVE (1u << 2)
#define FLAG_QUEUED (1u << 3)

#define VIDEO_DURATION_TYPE (video_duration_get_type())
G_DECLARE_FINAL_TYPE(VideoDuration, video_duration, VIDEO, DURATION, GObject)

struct _VideoDuration {
  GObject parent_instance;
};

static void column_provider_iface_init(NautilusColumnProviderInterface *iface);
static void info_provider_iface_init(NautilusInfoProviderInterface *iface);

G_DEFINE_DYNAMIC_TYPE_EXTENDED(
    VideoDuration, video_duration, G_TYPE_OBJECT, 0,
    G_IMPLEMENT_INTERFACE_DYNAMIC(NAUTILUS_TYPE_COLUMN_PROVIDER,
                                  column_provider_iface_init)
        G_IMPLEMENT_INTERFACE_DYNAMIC(NAUTILUS_TYPE_INFO_PROVIDER,
                                      info_provider_iface_init))

/* ------------------- Cache Entry ------------------- */
typedef struct {
  guint64 *keyp;
  guint64 mtime;
  guint64 size;

  guint32 seconds;
  guint32 flags;

  gint64 negative_until_us;
  gint64 last_request_us;

  GList *lru_link; /* O(1) */
} CacheEntry;

/* ------------------- Job ------------------- */
typedef struct {
  NautilusFileInfo *file;
  gchar *path;
  guint64 key;
} Job;

/* ------------------- Globals ------------------- */
static GThread **workers = NULL;
static gint worker_count = 0;
static GQueue *pending = NULL;
static GCond pending_cond;
static guint queue_max = DEFAULT_QUEUE_MAX;
static GMutex lock;

static GHashTable *entries = NULL; /* key(uint64*) -> CacheEntry* */
static GQueue *lru = NULL;

static volatile gint shutting_down = 0;
static guint cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;
static guint debounce_ms = DEFAULT_DEBOUNCE_MS;
static guint negative_ttl_sec = DEFAULT_NEGATIVE_TTL_SEC;
static guint evict_ratio_percent = DEFAULT_EVICT_RATIO_PERCENT;
static guint drop_negative_ttl_ms = DEFAULT_DROP_NEGATIVE_TTL_MS;

/* Helper config */
static guint helper_max = DEFAULT_HELPER_MAX;
static guint helper_timeout_ms = DEFAULT_HELPER_TIMEOUT_MS;
static guint helper_retry_timeout_ms = DEFAULT_HELPER_RETRY_TIMEOUT_MS;
static guint timeout_negative_ttl_sec = DEFAULT_TIMEOUT_NEG_TTL_SEC;
static guint helper_reqs_before_restart = DEFAULT_HELPER_REQS_BEFORE_RESTART;

/* ------------------- Helpers ------------------- */
static gboolean is_media_file(const gchar *name) {
  if (!name)
    return FALSE;
  const gchar *ext = strrchr(name, '.');
  if (!ext)
    return FALSE;
  ext++;
  const gchar *media_exts[] = {
      "mp4", "mkv",  "avi",  "mov", "webm", "flv",  "wmv",  "m4v", "mpeg",
      "mpg", "m2ts", "ts",   "3gp", "ogv",  "mp3",  "flac", "wav", "aac",
      "m4a", "ogg",  "opus", "wma", "alac", "aiff", NULL};
  for (int i = 0; media_exts[i]; i++)
    if (g_ascii_strcasecmp(ext, media_exts[i]) == 0)
      return TRUE;
  return FALSE;
}

static gboolean is_audio_ext(const gchar *name) {
  if (!name)
    return FALSE;
  const gchar *ext = strrchr(name, '.');
  if (!ext)
    return FALSE;
  ext++;
  const gchar *audio_exts[] = {"mp3",  "flac", "wav",  "aac",  "m4a", "ogg",
                               "opus", "wma",  "alac", "aiff", NULL};
  for (int i = 0; audio_exts[i]; i++)
    if (g_ascii_strcasecmp(ext, audio_exts[i]) == 0)
      return TRUE;
  return FALSE;
}

static gchar *format_seconds(guint32 sec, gboolean is_audio) {
  if (sec == 0)
    return g_strdup("-");
  if (is_audio && sec < 3600) {
    guint32 m = sec / 60, s = sec % 60;
    return g_strdup_printf("%02u:%02u", m, s);
  }
  guint32 h = sec / 3600;
  guint32 m = (sec % 3600) / 60;
  guint32 s = sec % 60;
  return g_strdup_printf("%u:%02u:%02u", h, m, s);
}

/* ------------------- key(uint64) ------------------- */
static inline guint64 mix64(guint64 x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}
static guint64 make_key_u64(const gchar *path, guint64 mtime, guint64 size) {
  guint32 h1 = g_str_hash(path);
  guint64 x =
      ((guint64)h1 << 32) ^ (mtime * 1315423911ULL) ^ (size * 2654435761ULL);
  return mix64(x);
}
static guint key_u64_hash(gconstpointer p) {
  const guint64 v = *(const guint64 *)p;
  return (guint)(v ^ (v >> 32));
}
static gboolean key_u64_equal(gconstpointer a, gconstpointer b) {
  return (*(const guint64 *)a) == (*(const guint64 *)b);
}

/* ------------------- LRU ------------------- */
static void lru_touch_unlocked(CacheEntry *e) {
  if (!e)
    return;
  if (e->lru_link) {
    g_queue_unlink(lru, e->lru_link);
    g_queue_push_tail_link(lru, e->lru_link);
  } else {
    g_queue_push_tail(lru, e);
    e->lru_link = g_queue_peek_tail_link(lru);
  }
}

static void entry_destroy(gpointer data) {
  CacheEntry *e = data;
  if (!e)
    return;
  e->lru_link = NULL;
  g_free(e);
}

static void cache_entry_remove_unlocked(CacheEntry *e) {
  if (!e)
    return;
  if (e->lru_link) {
    g_queue_unlink(lru, e->lru_link);
    e->lru_link = NULL;
  }
  g_hash_table_remove(entries, e->keyp);
}

static void evict_if_needed_unlocked(void) {
  guint sz = g_hash_table_size(entries);
  if (sz <= cache_max_entries)
    return;

  guint target = (cache_max_entries * evict_ratio_percent) / 100;
  if (target < 1)
    target = 1;

  guint tries = 0;
  guint max_tries = (guint)g_queue_get_length(lru);
  if (max_tries < 1)
    max_tries = 1;

  while (sz > target && tries < max_tries) {
    CacheEntry *old = g_queue_pop_head(lru);
    if (!old)
      break;

    old->lru_link = NULL;

    if (old->flags & FLAG_INFLIGHT) {
      g_queue_push_tail(lru, old);
      old->lru_link = g_queue_peek_tail_link(lru);
      tries++;
      continue;
    }

    g_hash_table_remove(entries, old->keyp);
    sz--;
    tries++;
  }
}

/* ------------------- File stat ------------------- */
static gboolean get_local_path_and_stat(const gchar *uri, gchar **out_path,
                                        guint64 *out_mtime, guint64 *out_size) {
  *out_path = NULL;
  *out_mtime = 0;
  *out_size = 0;
  if (!uri || !g_str_has_prefix(uri, "file://"))
    return FALSE;

  gchar *path = g_filename_from_uri(uri, NULL, NULL);
  if (!path)
    return FALSE;

  GFile *gf = g_file_new_for_path(path);
  GFileInfo *fi = g_file_query_info(
      gf, G_FILE_ATTRIBUTE_TIME_MODIFIED "," G_FILE_ATTRIBUTE_STANDARD_SIZE,
      G_FILE_QUERY_INFO_NONE, NULL, NULL);

  if (fi) {
    *out_mtime =
        g_file_info_get_attribute_uint64(fi, G_FILE_ATTRIBUTE_TIME_MODIFIED);
    *out_size =
        g_file_info_get_attribute_uint64(fi, G_FILE_ATTRIBUTE_STANDARD_SIZE);
    g_object_unref(fi);
  }
  g_object_unref(gf);

  *out_path = path;
  return TRUE;
}

/* ------------------- Env ------------------- */
static guint read_env_uint(const gchar *name, guint fallback, guint min,
                           guint max) {
  const gchar *env = g_getenv(name);
  if (!env || !*env)
    return fallback;
  gchar *end = NULL;
  guint64 v = g_ascii_strtoull(env, &end, 10);
  if (end == env || v == 0)
    return fallback;
  if (v < min)
    v = min;
  if (v > max)
    v = max;
  return (guint)v;
}

static void init_limits_from_env(void) {
  cache_max_entries =
      read_env_uint("NAUTILUS_VD_CACHE", DEFAULT_CACHE_MAX_ENTRIES, 1, 200000);
  queue_max =
      read_env_uint("NAUTILUS_VD_QUEUE_MAX", DEFAULT_QUEUE_MAX, 1, 200000);
  debounce_ms =
      read_env_uint("NAUTILUS_VD_DEBOUNCE_MS", DEFAULT_DEBOUNCE_MS, 0, 5000);
  negative_ttl_sec =
      read_env_uint("NAUTILUS_VD_NEG_TTL", DEFAULT_NEGATIVE_TTL_SEC, 1, 86400);
  evict_ratio_percent = read_env_uint("NAUTILUS_VD_EVICT_RATIO",
                                      DEFAULT_EVICT_RATIO_PERCENT, 10, 95);
  drop_negative_ttl_ms = read_env_uint("NAUTILUS_VD_DROP_NEG_TTL_MS",
                                       DEFAULT_DROP_NEGATIVE_TTL_MS, 0, 60000);

  helper_max =
      read_env_uint("NAUTILUS_VD_HELPER_MAX", DEFAULT_HELPER_MAX, 1, 16);
  helper_timeout_ms = read_env_uint("NAUTILUS_VD_FF_TIMEOUT_MS",
                                    DEFAULT_HELPER_TIMEOUT_MS, 100, 60000);
  helper_retry_timeout_ms =
      read_env_uint("NAUTILUS_VD_FF_RETRY_TIMEOUT_MS",
                    DEFAULT_HELPER_RETRY_TIMEOUT_MS, 200, 180000);
  timeout_negative_ttl_sec = read_env_uint(
      "NAUTILUS_VD_TIMEOUT_NEG_TTL_SEC", DEFAULT_TIMEOUT_NEG_TTL_SEC, 0, 3600);
  helper_reqs_before_restart =
      read_env_uint("NAUTILUS_VD_HELPER_REQS_BEFORE_RESTART",
                    DEFAULT_HELPER_REQS_BEFORE_RESTART, 50, 100000);
}

/* ------------------- Main thread update ------------------- */
typedef struct {
  NautilusFileInfo *file;
  guint32 seconds;
  gboolean negative;
  gboolean audio;
} MainUpdate;

static gboolean apply_update_main(gpointer data) {
  MainUpdate *u = data;
  if (!u)
    return G_SOURCE_REMOVE;

  if (!g_atomic_int_get(&shutting_down)) {
    gchar *dur = NULL;
    if (!u->negative && u->seconds > 0)
      dur = format_seconds(u->seconds, u->audio);
    else
      dur = g_strdup("-");

    nautilus_file_info_add_string_attribute(u->file, ATTR_KEY, dur);
    g_free(dur);
  }

  g_object_unref(u->file);
  g_free(u);
  return G_SOURCE_REMOVE;
}

static void post_update_to_main(NautilusFileInfo *file, guint32 seconds,
                                gboolean negative, gboolean audio) {
  if (g_atomic_int_get(&shutting_down))
    return;

  MainUpdate *u = g_new0(MainUpdate, 1);
  u->file = g_object_ref(file);
  u->seconds = seconds;
  u->negative = negative;
  u->audio = audio;

  g_main_context_invoke(NULL, apply_update_main, u);
}

/* ------------------- Helper Pool ------------------- */
typedef struct {
  GSubprocess *proc;
  GDataInputStream *din;
  GOutputStream *out;
  guint req_count;
} HelperInstance;

typedef struct {
  GMutex m;
  GCond c;
  GQueue *free_list; /* HelperInstance* */
  gint total_created;
  gint capacity;
  gboolean inited;
  gchar *helper_path;
} HelperPool;

static HelperPool helper_pool;

static gchar *resolve_helper_path(void) {
  const gchar *env = g_getenv("NAUTILUS_VD_HELPER");
  if (env && *env)
    return g_strdup(env);
  return g_strdup(VD_HELPER_PATH);
}

static void helper_instance_destroy(HelperInstance *h) {
  if (!h)
    return;
  if (h->proc)
    g_object_unref(h->proc);
  if (h->din)
    g_object_unref(h->din);
  /* out is owned by proc's stdout? Actually from get_stdin_pipe -> new ref */
  if (h->out)
    g_object_unref(h->out);
  g_free(h);
}

static gboolean helper_instance_is_alive(HelperInstance *h) {
  if (!h || !h->proc)
    return FALSE;
  return g_subprocess_get_if_exited(h->proc) ? FALSE : TRUE;
}

static HelperInstance *helper_instance_spawn(const gchar *helper_path) {
  GError *err = NULL;

  const gchar *argv[] = {helper_path, NULL};

  GSubprocess *p = g_subprocess_newv(argv,
                                     G_SUBPROCESS_FLAGS_STDIN_PIPE |
                                         G_SUBPROCESS_FLAGS_STDOUT_PIPE |
                                         G_SUBPROCESS_FLAGS_STDERR_SILENCE,
                                     &err);

  if (!p) {
    g_message("VD: spawn helper failed: %s", err ? err->message : "(unknown)");
    g_clear_error(&err);
    return NULL;
  }

  GInputStream *stdout_is = g_subprocess_get_stdout_pipe(p);
  GOutputStream *stdin_os = g_subprocess_get_stdin_pipe(p);

  HelperInstance *h = g_new0(HelperInstance, 1);
  h->proc = p;
  h->din = g_data_input_stream_new(stdout_is);
  h->out = g_object_ref(stdin_os);
  h->req_count = 0;

  /* line length cap to prevent runaway */
  g_data_input_stream_set_newline_type(h->din, G_DATA_STREAM_NEWLINE_TYPE_LF);

  return h;
}

static void helper_pool_init(HelperPool *p, gint capacity) {
  g_mutex_init(&p->m);
  g_cond_init(&p->c);
  p->free_list = g_queue_new();
  p->total_created = 0;
  p->capacity = capacity;
  p->inited = TRUE;
  p->helper_path = resolve_helper_path();
}

static void helper_pool_wake_all(HelperPool *p) {
  if (!p || !p->inited)
    return;
  g_mutex_lock(&p->m);
  g_cond_broadcast(&p->c);
  g_mutex_unlock(&p->m);
}

static void helper_pool_clear(HelperPool *p) {
  if (!p || !p->inited)
    return;

  g_mutex_lock(&p->m);
  while (!g_queue_is_empty(p->free_list)) {
    HelperInstance *h = g_queue_pop_head(p->free_list);
    helper_instance_destroy(h);
  }
  g_queue_free(p->free_list);
  p->free_list = NULL;
  p->total_created = 0;
  p->capacity = 0;
  p->inited = FALSE;
  g_mutex_unlock(&p->m);

  g_free(p->helper_path);

  g_cond_clear(&p->c);
  g_mutex_clear(&p->m);
}

static HelperInstance *helper_pool_acquire(HelperPool *p) {
  if (!p || !p->inited)
    return NULL;

  g_mutex_lock(&p->m);
  while (!g_atomic_int_get(&shutting_down)) {
    if (!g_queue_is_empty(p->free_list)) {
      HelperInstance *h = g_queue_pop_head(p->free_list);
      g_mutex_unlock(&p->m);
      return h;
    }

    if (p->total_created < p->capacity) {
      p->total_created++;
      gchar *hp = g_strdup(p->helper_path);
      g_mutex_unlock(&p->m);

      HelperInstance *h = helper_instance_spawn(hp);
      g_free(hp);

      if (!h) {
        g_mutex_lock(&p->m);
        p->total_created--;
        g_mutex_unlock(&p->m);
        return NULL;
      }
      return h;
    }

    g_cond_wait(&p->c, &p->m);
  }
  g_mutex_unlock(&p->m);
  return NULL;
}

static void helper_pool_release(HelperPool *p, HelperInstance *h) {
  if (!p || !p->inited || !h)
    return;

  g_mutex_lock(&p->m);
  g_queue_push_tail(p->free_list, h);
  g_cond_signal(&p->c);
  g_mutex_unlock(&p->m);
}

static guint32 parse_ok_line_seconds(const gchar *line) {
  if (!line)
    return 0;
  /* expected: "OK <sec>" or "OK 0" */
  if (g_str_has_prefix(line, "OK")) {
    const gchar *p = line + 2;
    while (*p == ' ' || *p == '\t')
      p++;
    if (!*p)
      return 0;
    guint64 v = g_ascii_strtoull(p, NULL, 10);
    if (v > G_MAXUINT32)
      return G_MAXUINT32;
    return (guint32)v;
  }
  return 0;
}

typedef struct {
  guint32 seconds;
  gboolean timed_out;
} DurationResult;

/* One request to helper. Protocol:
 *   REQ <timeout_ms> <retry_timeout_ms> <b64(path)>\n
 *   -> OK <seconds>\n
 * If helper detects timeout in pass1 and pass2, it still replies OK 0.
 * We mark timed_out if we receive "OK 0" AND helper also prints "TO"?
 * To keep protocol simple, helper will output "OK <sec> <flags>" where flags
 * include 'T' if timed out. We'll parse it.
 */
static DurationResult helper_request_duration(HelperInstance *h,
                                              const gchar *path) {
  DurationResult r = {0, FALSE};
  if (!h || !path)
    return r;

  /* base64 encode path (safe line protocol) */
  gchar *b64 = g_base64_encode((const guchar *)path, (gsize)strlen(path));
  gchar *line = g_strdup_printf("REQ %u %u %s\n", helper_timeout_ms,
                                helper_retry_timeout_ms, b64);
  g_free(b64);

  GError *err = NULL;
  gsize written = 0;
  gboolean ok = g_output_stream_write_all(h->out, line, strlen(line), &written,
                                          NULL, &err);
  g_free(line);

  if (!ok) {
    g_clear_error(&err);
    return r;
  }
  ok = g_output_stream_flush(h->out, NULL, &err);
  if (!ok) {
    g_clear_error(&err);
    return r;
  }

  /* Read one line response */
  gsize len = 0;
  gchar *resp = g_data_input_stream_read_line(h->din, &len, NULL, &err);
  if (!resp) {
    g_clear_error(&err);
    return r;
  }

  /* expected: "OK <sec> <flags>" */
  /* flags: may contain 'T' if timed out happened (even if sec==0) */
  if (g_str_has_prefix(resp, "OK")) {
    gchar **parts = g_strsplit(resp, " ", 0);
    if (parts && parts[1]) {
      guint64 v = g_ascii_strtoull(parts[1], NULL, 10);
      if (v > G_MAXUINT32)
        v = G_MAXUINT32;
      r.seconds = (guint32)v;
    }
    if (parts && parts[2]) {
      if (strchr(parts[2], 'T'))
        r.timed_out = TRUE;
    }
    g_strfreev(parts);
  }

  g_free(resp);

  return r;
}

static void helper_instance_maybe_restart(HelperPool *p, HelperInstance *h) {
  if (!h)
    return;

  if (!helper_instance_is_alive(h)) {
    helper_instance_destroy(h);
    /* total_created stays same; we will spawn again on next acquire */
    g_mutex_lock(&p->m);
    p->total_created--;
    g_mutex_unlock(&p->m);
    return;
  }

  h->req_count++;
  if (h->req_count >= helper_reqs_before_restart) {
    /* graceful request: send QUIT */
    GError *err = NULL;
    const gchar *q = "QUIT\n";
    g_output_stream_write_all(h->out, q, strlen(q), NULL, NULL, &err);
    g_clear_error(&err);
    g_output_stream_flush(h->out, NULL, NULL);

    helper_instance_destroy(h);
    g_mutex_lock(&p->m);
    p->total_created--;
    g_mutex_unlock(&p->m);
  }
}

/* Wrapper that uses helper pool */
static DurationResult get_duration_seconds_via_helper(const gchar *path) {
  DurationResult r = {0, FALSE};
  if (!path)
    return r;

  HelperInstance *h = helper_pool_acquire(&helper_pool);
  if (!h)
    return r;

  r = helper_request_duration(h, path);

  /* if helper broken, drop it; else return to pool */
  if (!helper_instance_is_alive(h)) {
    helper_instance_destroy(h);
    g_mutex_lock(&helper_pool.m);
    helper_pool.total_created--;
    g_mutex_unlock(&helper_pool.m);
  } else {
    /* optional: periodic restart to cap helper memory */
    helper_instance_maybe_restart(&helper_pool, h);
    /* If it was destroyed in maybe_restart, don't release */
    if (helper_instance_is_alive(h)) {
      helper_pool_release(&helper_pool, h);
    }
  }

  return r;
}

/* ------------------- Job / Worker ------------------- */
static void job_free(Job *job) {
  if (!job)
    return;
  if (job->file)
    g_object_unref(job->file);
  g_free(job->path);
  g_free(job);
}

static void worker_process(Job *job) {
  if (!job)
    return;
  if (g_atomic_int_get(&shutting_down)) {
    job_free(job);
    return;
  }

  DurationResult dr = get_duration_seconds_via_helper(job->path);
  guint32 seconds = dr.seconds;
  gboolean negative = (seconds == 0);
  gboolean timeout_fail = (seconds == 0 && dr.timed_out);

  gchar *base = g_path_get_basename(job->path);
  gboolean audio = is_audio_ext(base);
  g_free(base);

  gint64 now_us = g_get_monotonic_time();

  g_mutex_lock(&lock);

  gpointer fk = NULL, fv = NULL;
  if (g_hash_table_lookup_extended(entries, &job->key, &fk, &fv)) {
    CacheEntry *e = fv;

    e->flags &= ~(FLAG_INFLIGHT | FLAG_QUEUED);

    if (!negative) {
      e->flags &= ~FLAG_NEGATIVE;
      e->flags |= FLAG_HAS_VALUE;
      e->seconds = seconds;
      e->negative_until_us = 0;
    } else {
      e->flags &= ~FLAG_HAS_VALUE;
      e->flags |= FLAG_NEGATIVE;
      e->seconds = 0;

      /* HDD-friendly: timeout failure -> short negative TTL */
      guint ttl = negative_ttl_sec;
      if (timeout_fail)
        ttl = timeout_negative_ttl_sec;

      if (ttl == 0)
        e->negative_until_us = now_us;
      else
        e->negative_until_us = now_us + (gint64)ttl * G_USEC_PER_SEC;
    }

    lru_touch_unlocked(e);
    evict_if_needed_unlocked();
  }

  g_mutex_unlock(&lock);

  post_update_to_main(job->file, seconds, negative, audio);
  job_free(job);
}

static gpointer worker_thread(gpointer data) {
  (void)data;
  for (;;) {
    g_mutex_lock(&lock);
    while (!g_atomic_int_get(&shutting_down) && g_queue_is_empty(pending))
      g_cond_wait(&pending_cond, &lock);

    if (g_atomic_int_get(&shutting_down)) {
      g_mutex_unlock(&lock);
      break;
    }

    Job *job = g_queue_pop_head(pending);
    g_mutex_unlock(&lock);

    if (job)
      worker_process(job);
  }
  return NULL;
}

/* ------------------- Schedule ------------------- */
static void schedule_duration_job(NautilusFileInfo *file) {
  if (g_atomic_int_get(&shutting_down))
    return;

  gchar *name = nautilus_file_info_get_name(file);
  if (!is_media_file(name)) {
    g_free(name);
    return;
  }
  g_free(name);

  gchar *uri = nautilus_file_info_get_uri(file);
  if (!uri)
    return;

  gchar *path = NULL;
  guint64 mtime = 0, size = 0;
  if (!get_local_path_and_stat(uri, &path, &mtime, &size)) {
    g_free(uri);
    return;
  }
  g_free(uri);

  guint64 key = make_key_u64(path, mtime, size);
  gint64 now_us = g_get_monotonic_time();
  gboolean audio = is_audio_ext(path);

  g_mutex_lock(&lock);

  CacheEntry *e = NULL;
  gpointer fk = NULL, fv = NULL;
  if (g_hash_table_lookup_extended(entries, &key, &fk, &fv)) {
    e = fv;
    if (e->mtime != mtime || e->size != size) {
      cache_entry_remove_unlocked(e);
      e = NULL;
    }
  }

  gboolean is_new = FALSE;
  if (!e) {
    guint64 *keyp = g_new(guint64, 1);
    *keyp = key;

    e = g_new0(CacheEntry, 1);
    e->keyp = keyp;
    e->mtime = mtime;
    e->size = size;
    e->seconds = 0;
    e->flags = 0;
    e->negative_until_us = 0;
    e->last_request_us = 0;
    e->lru_link = NULL;

    g_hash_table_insert(entries, keyp, e);
    lru_touch_unlocked(e);
    evict_if_needed_unlocked();
    is_new = TRUE;
  }

  /* debounce */
  if (!is_new && debounce_ms > 0 && e->last_request_us) {
    gint64 dt = now_us - e->last_request_us;
    if (dt < (gint64)debounce_ms * 1000) {
      if (e->flags & FLAG_HAS_VALUE) {
        gchar *dur = format_seconds(e->seconds, audio);
        nautilus_file_info_add_string_attribute(file, ATTR_KEY, dur);
        g_free(dur);
        lru_touch_unlocked(e);
        g_mutex_unlock(&lock);
        g_free(path);
        return;
      }
      if ((e->flags & FLAG_NEGATIVE) && now_us < e->negative_until_us) {
        nautilus_file_info_add_string_attribute(file, ATTR_KEY, "-");
        lru_touch_unlocked(e);
        g_mutex_unlock(&lock);
        g_free(path);
        return;
      }
      if (e->flags & FLAG_INFLIGHT) {
        g_mutex_unlock(&lock);
        g_free(path);
        return;
      }
    }
  }
  e->last_request_us = now_us;

  /* cache hit */
  if (e->flags & FLAG_HAS_VALUE) {
    gchar *dur = format_seconds(e->seconds, audio);
    nautilus_file_info_add_string_attribute(file, ATTR_KEY, dur);
    g_free(dur);
    lru_touch_unlocked(e);
    g_mutex_unlock(&lock);
    g_free(path);
    return;
  }

  /* negative cache */
  if ((e->flags & FLAG_NEGATIVE) && now_us < e->negative_until_us) {
    nautilus_file_info_add_string_attribute(file, ATTR_KEY, "-");
    lru_touch_unlocked(e);
    g_mutex_unlock(&lock);
    g_free(path);
    return;
  }

  /* inflight */
  if (e->flags & FLAG_INFLIGHT) {
    g_mutex_unlock(&lock);
    g_free(path);
    return;
  }

  e->flags |= FLAG_INFLIGHT;

  /* ✅ no placeholder */

  if (!(e->flags & FLAG_QUEUED)) {
    e->flags |= FLAG_QUEUED;

    Job *job = g_new0(Job, 1);
    job->file = g_object_ref(file);
    job->key = key;
    job->path = g_strdup(path);

    Job *dropped = NULL;
    if (pending && g_queue_get_length(pending) >= queue_max) {
      dropped = g_queue_pop_head(pending);
      if (dropped) {
        gpointer dfk = NULL, dfv = NULL;
        if (g_hash_table_lookup_extended(entries, &dropped->key, &dfk, &dfv)) {
          CacheEntry *de = dfv;
          de->flags &= ~(FLAG_INFLIGHT | FLAG_QUEUED);

          if (drop_negative_ttl_ms > 0) {
            de->flags &= ~FLAG_HAS_VALUE;
            de->flags |= FLAG_NEGATIVE;
            de->seconds = 0;
            de->negative_until_us =
                now_us + (gint64)drop_negative_ttl_ms * 1000;
            lru_touch_unlocked(de);
          }
        }
      }
    }

    g_queue_push_tail(pending, job);
    g_cond_signal(&pending_cond);

    if (dropped)
      job_free(dropped);
  }

  g_mutex_unlock(&lock);
  g_free(path);
}

static NautilusOperationResult video_duration_update_file_info(
    NautilusInfoProvider *provider, NautilusFileInfo *file,
    GClosure *update_complete, NautilusOperationHandle **handle) {
  (void)provider;
  (void)update_complete;
  (void)handle;

  schedule_duration_job(file);
  return NAUTILUS_OPERATION_COMPLETE;
}

static void info_provider_iface_init(NautilusInfoProviderInterface *iface) {
  iface->update_file_info = video_duration_update_file_info;
}

/* ------------------- ColumnProvider ------------------- */
static GList *video_duration_get_columns(NautilusColumnProvider *provider) {
  (void)provider;
  NautilusColumn *col =
      nautilus_column_new(COLUMN_ID, ATTR_KEY, "时长", "媒体时长（视频/音频）");
  return g_list_append(NULL, col);
}
static void column_provider_iface_init(NautilusColumnProviderInterface *iface) {
  iface->get_columns = video_duration_get_columns;
}

/* ------------------- Class init ------------------- */
static void video_duration_class_init(VideoDurationClass *klass) {
  (void)klass;
}
static void video_duration_class_finalize(VideoDurationClass *klass) {
  (void)klass;
}
static void video_duration_init(VideoDuration *self) { (void)self; }

/* ------------------- Module entry ------------------- */
void nautilus_module_initialize(GTypeModule *module) {
  video_duration_register_type(module);

  g_message("%s", VD_BUILD_TAG);

  g_atomic_int_set(&shutting_down, 0);
  init_limits_from_env();

  g_mutex_init(&lock);
  g_cond_init(&pending_cond);

  entries =
      g_hash_table_new_full(key_u64_hash, key_u64_equal, g_free, entry_destroy);
  lru = g_queue_new();
  pending = g_queue_new();

  helper_pool_init(&helper_pool, (gint)helper_max);

  /* threads default: <= 8 */
  gint n_threads = (gint)g_get_num_processors();
  if (n_threads < 1)
    n_threads = 1;
  gint capped = n_threads > 8 ? 8 : n_threads;

  worker_count =
      (gint)read_env_uint("NAUTILUS_VD_THREADS", (guint)capped, 1, 128);

  g_message("VD: helperpool threads=%d helper_max=%u cache_max=%u queue_max=%u "
            "debounce_ms=%u neg_ttl=%u evict_ratio=%u drop_neg_ttl_ms=%u "
            "ff_timeout=%u ff_retry=%u timeout_neg_ttl=%u "
            "helper_reqs_restart=%u helper_path=%s",
            worker_count, helper_max, cache_max_entries, queue_max, debounce_ms,
            negative_ttl_sec, evict_ratio_percent, drop_negative_ttl_ms,
            helper_timeout_ms, helper_retry_timeout_ms,
            timeout_negative_ttl_sec, helper_reqs_before_restart,
            helper_pool.helper_path);

  workers = g_new0(GThread *, worker_count);
  for (gint i = 0; i < worker_count; i++)
    workers[i] = g_thread_new("vd-worker", worker_thread, NULL);
}

void nautilus_module_shutdown(void) {
  g_atomic_int_set(&shutting_down, 1);

  /* wake pending workers */
  g_mutex_lock(&lock);
  g_cond_broadcast(&pending_cond);
  g_mutex_unlock(&lock);

  /* wake helper pool waiters */
  helper_pool_wake_all(&helper_pool);

  if (workers) {
    for (gint i = 0; i < worker_count; i++)
      if (workers[i])
        g_thread_join(workers[i]);
    g_free(workers);
    workers = NULL;
    worker_count = 0;
  }

  g_mutex_lock(&lock);

  if (pending) {
    while (!g_queue_is_empty(pending)) {
      Job *job = g_queue_pop_head(pending);
      if (job)
        job_free(job);
    }
    g_queue_free(pending);
    pending = NULL;
  }

  if (lru) {
    g_queue_free(lru);
    lru = NULL;
  }
  if (entries) {
    g_hash_table_destroy(entries);
    entries = NULL;
  }

  g_mutex_unlock(&lock);

  helper_pool_clear(&helper_pool);

  g_cond_clear(&pending_cond);
  g_mutex_clear(&lock);
}

void nautilus_module_list_types(const GType **types, int *num_types) {
  static GType type_list[1];
  type_list[0] = VIDEO_DURATION_TYPE;
  *types = type_list;
  *num_types = 1;
}
