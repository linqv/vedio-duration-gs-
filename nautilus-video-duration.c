#include <gio/gio.h>
#include <glib-object.h>
#include <glib.h>
#include <nautilus-extension.h>

#include <gst/gst.h>
#include <gst/pbutils/pbutils.h>

#define VD_BUILD_TAG "VD_BUILD_TAG: COMPLETE-async-2026-01-10-1628-final"

#define COLUMN_ID "video-duration::duration"
#define ATTR_KEY "video-duration::duration"

#define DEFAULT_CACHE_MAX_ENTRIES 512
#define DEFAULT_QUEUE_MAX 256
#define DEFAULT_DEBOUNCE_MS 250
#define DEFAULT_NEGATIVE_TTL_SEC 600 /* 10 min */
#define DEFAULT_EVICT_RATIO_PERCENT 70

#define DEFAULT_DROP_NEGATIVE_TTL_MS 2000 /* drop 后短暂负缓存 */

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
  guint64 *keyp; /* hashtable key pointer */
  guint64 mtime; /* collision check */
  guint64 size;  /* collision check */

  guint32 seconds; /* cached duration */
  guint32 flags;

  gint64 negative_until_us; /* negative cache valid until */
  gint64 last_request_us;   /* debounce */

  GList *lru_link; /* ✅ O(1) LRU link */
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

/* -------- GStreamer init -------- */
static gsize gst_inited = 0;

/* ------------------- Discoverer Pool (NEW) ------------------- */
typedef struct {
  GMutex m;
  GCond c;
  GQueue *free_list;  /* GstDiscoverer* */
  gint total_created; /* total created discoverers */
  gint capacity;      /* max discoverers */
  guint64 timeout_ns; /* discoverer timeout */
  gboolean inited;
} DiscovererPool;

static DiscovererPool pool_5s;
static DiscovererPool pool_10s;
static gint discover_max = 5;

static guint64 discover_timeout_ns(void) { return 5 * GST_SECOND; }
static guint64 discover_retry_timeout_ns(void) { return 10 * GST_SECOND; }

static void ensure_gst_init_once(void) {
  if (g_once_init_enter(&gst_inited)) {
    gst_init(NULL, NULL);
    g_once_init_leave(&gst_inited, 1);
  }
}

static void discoverer_pool_init(DiscovererPool *p, gint capacity,
                                 guint64 timeout_ns) {
  g_mutex_init(&p->m);
  g_cond_init(&p->c);
  p->free_list = g_queue_new();
  p->total_created = 0;
  p->capacity = capacity;
  p->timeout_ns = timeout_ns;
  p->inited = TRUE;
}

static GstDiscoverer *discoverer_pool_acquire(DiscovererPool *p) {
  if (!p || !p->inited)
    return NULL;

  g_mutex_lock(&p->m);

  while (!g_atomic_int_get(&shutting_down)) {
    if (!g_queue_is_empty(p->free_list)) {
      GstDiscoverer *dc = g_queue_pop_head(p->free_list);
      g_mutex_unlock(&p->m);
      return dc;
    }

    if (p->total_created < p->capacity) {
      p->total_created++;
      g_mutex_unlock(&p->m);

      GError *err = NULL;
      GstDiscoverer *dc = gst_discoverer_new(p->timeout_ns, &err);
      if (!dc) {
        g_message("VD: gst_discoverer_new(%lu ns) failed: %s",
                  (unsigned long)p->timeout_ns,
                  err ? err->message : "(unknown)");
        g_clear_error(&err);

        /* rollback created count */
        g_mutex_lock(&p->m);
        p->total_created--;
        g_mutex_unlock(&p->m);

        return NULL;
      }
      return dc;
    }

    /* wait for release */
    g_cond_wait(&p->c, &p->m);
  }

  g_mutex_unlock(&p->m);
  return NULL;
}

static void discoverer_pool_release(DiscovererPool *p, GstDiscoverer *dc) {
  if (!p || !p->inited || !dc)
    return;

  g_mutex_lock(&p->m);
  g_queue_push_tail(p->free_list, dc);
  g_cond_signal(&p->c);
  g_mutex_unlock(&p->m);
}

static void discoverer_pool_clear(DiscovererPool *p) {
  if (!p || !p->inited)
    return;

  g_mutex_lock(&p->m);

  while (!g_queue_is_empty(p->free_list)) {
    GstDiscoverer *dc = g_queue_pop_head(p->free_list);
    if (dc)
      g_object_unref(dc);
  }

  g_queue_free(p->free_list);
  p->free_list = NULL;
  p->total_created = 0;
  p->capacity = 0;
  p->inited = FALSE;

  g_mutex_unlock(&p->m);

  g_cond_clear(&p->c);
  g_mutex_clear(&p->m);
}

static double discover_once_pool(const gchar *uri, DiscovererPool *p) {
  GstDiscoverer *dc = discoverer_pool_acquire(p);
  if (!dc)
    return 0.0;

  GError *err = NULL;
  GstDiscovererInfo *info = gst_discoverer_discover_uri(dc, uri, &err);

  discoverer_pool_release(p, dc);

  if (!info) {
    g_clear_error(&err);
    return 0.0;
  }

  if (gst_discoverer_info_get_result(info) != GST_DISCOVERER_OK) {
    g_clear_error(&err);
    g_object_unref(info);
    return 0.0;
  }

  GstClockTime dur = gst_discoverer_info_get_duration(info);
  g_object_unref(info);

  if (dur == GST_CLOCK_TIME_NONE || dur == 0)
    return 0.0;

  return (double)dur / (double)GST_SECOND;
}

static guint32 get_duration_seconds(const gchar *path) {
  if (!path)
    return 0;

  ensure_gst_init_once();

  gchar *uri = g_filename_to_uri(path, NULL, NULL);
  if (!uri)
    return 0;

  double sec = discover_once_pool(uri, &pool_5s);

  if (sec <= 0.0 && !g_atomic_int_get(&shutting_down)) {
    g_usleep(200 * 1000);
    sec = discover_once_pool(uri, &pool_10s);
  }

  g_free(uri);

  if (sec <= 0.0)
    return 0;
  if (sec > (double)G_MAXUINT32)
    return G_MAXUINT32;

  return (guint32)(sec + 0.5);
}

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

/* ------------------- key(uint64) + collision check ------------------- */
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

/* hashtable key funcs (uint64*) */
static guint key_u64_hash(gconstpointer p) {
  const guint64 v = *(const guint64 *)p;
  return (guint)(v ^ (v >> 32));
}
static gboolean key_u64_equal(gconstpointer a, gconstpointer b) {
  return (*(const guint64 *)a) == (*(const guint64 *)b);
}

/* ------------------- LRU (O(1)) ------------------- */
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

/* entry destroy */
static void entry_destroy(gpointer data) {
  CacheEntry *e = data;
  if (!e)
    return;

#ifdef G_ENABLE_DEBUG
  if (e->lru_link) {
    g_warning("VD: entry_destroy called while still linked in LRU (bug)!");
  }
#endif

  e->lru_link = NULL;
  g_free(e);
}

/* unified remove (lock held) */
static void cache_entry_remove_unlocked(CacheEntry *e) {
  if (!e)
    return;

  if (e->lru_link) {
    g_queue_unlink(lru, e->lru_link);
    e->lru_link = NULL;
  }

  g_hash_table_remove(entries, e->keyp);
}

/* improved evict: skip inflight */
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
  discover_max = (gint)read_env_uint("NAUTILUS_VD_DISCOVER_MAX", 5, 1, 32);
  debounce_ms =
      read_env_uint("NAUTILUS_VD_DEBOUNCE_MS", DEFAULT_DEBOUNCE_MS, 0, 5000);
  negative_ttl_sec =
      read_env_uint("NAUTILUS_VD_NEG_TTL", DEFAULT_NEGATIVE_TTL_SEC, 1, 86400);

  evict_ratio_percent = read_env_uint("NAUTILUS_VD_EVICT_RATIO",
                                      DEFAULT_EVICT_RATIO_PERCENT, 10, 95);

  drop_negative_ttl_ms = read_env_uint("NAUTILUS_VD_DROP_NEG_TTL_MS",
                                       DEFAULT_DROP_NEGATIVE_TTL_MS, 0, 60000);
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

  guint32 seconds = get_duration_seconds(job->path);
  gboolean negative = (seconds == 0);

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
      e->negative_until_us = now_us + (gint64)negative_ttl_sec * G_USEC_PER_SEC;
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

  nautilus_file_info_add_string_attribute(file, ATTR_KEY, "…");

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

  /* ✅ Discoverer pool init: capacity = discover_max */
  ensure_gst_init_once();
  discoverer_pool_init(&pool_5s, discover_max, discover_timeout_ns());
  discoverer_pool_init(&pool_10s, discover_max, discover_retry_timeout_ns());

  /* ✅ cap default threads */
  gint n_threads = (gint)g_get_num_processors();
  if (n_threads < 1)
    n_threads = 1;
  gint capped = n_threads;
  if (capped > 8)
    capped = 8;

  worker_count =
      (gint)read_env_uint("NAUTILUS_VD_THREADS", (guint)capped, 1, 128);

  g_message("VD: COMPLETE-async threads=%d discover_max=%d cache_max=%u "
            "queue_max=%u debounce_ms=%u neg_ttl=%u evict_ratio=%u "
            "drop_neg_ttl_ms=%u",
            worker_count, discover_max, cache_max_entries, queue_max,
            debounce_ms, negative_ttl_sec, evict_ratio_percent,
            drop_negative_ttl_ms);

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

  /* wake discoverer pool waiters */
  if (pool_5s.inited) {
    g_mutex_lock(&pool_5s.m);
    g_cond_broadcast(&pool_5s.c);
    g_mutex_unlock(&pool_5s.m);
  }
  if (pool_10s.inited) {
    g_mutex_lock(&pool_10s.m);
    g_cond_broadcast(&pool_10s.c);
    g_mutex_unlock(&pool_10s.m);
  }

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

  /* ✅ clear pools (unref all discoverers) */
  discoverer_pool_clear(&pool_5s);
  discoverer_pool_clear(&pool_10s);

  g_cond_clear(&pending_cond);
  g_mutex_clear(&lock);
}

void nautilus_module_list_types(const GType **types, int *num_types) {
  static GType type_list[1];
  type_list[0] = VIDEO_DURATION_TYPE;
  *types = type_list;
  *num_types = 1;
}
