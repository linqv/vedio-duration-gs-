#include <gio/gio.h>
#include <glib-object.h>
#include <glib.h>
#include <nautilus-extension.h>

#include <gst/gst.h>
#include <gst/pbutils/pbutils.h>

/*
 * ✅ Nautilus 4/49 强烈建议使用命名空间 attribute。
 * 我们让 COLUMN_ID 与 ATTR_KEY 完全一致，避免任何绑定错误。
 */
#define COLUMN_ID "video-duration::duration"
#define ATTR_KEY "video-duration::duration"

#define DEFAULT_CACHE_MAX_ENTRIES 4096

/* ------------------- Type ------------------- */

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

/* ------------------- Globals ------------------- */

typedef struct {
  NautilusFileInfo *file;
  gchar *path;
  guint64 mtime;
  guint64 size;
  gchar *key; /* key = path|mtime|size */
} Job;

static GThreadPool *pool = NULL;
static GMutex lock;

static GHashTable *cache = NULL;    /* key(str) -> dur(str) */
static GHashTable *inflight = NULL; /* key(str) -> 1 */

static volatile gint shutting_down = 0;
static guint cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;

/* LRU: queue holds cache key pointers (owned by cache) */
static GQueue *lru = NULL;
/* direct map: key_ptr -> node_ptr */
static GHashTable *lru_pos = NULL;

/* -------- GStreamer init -------- */
static gsize gst_inited = 0;

/* -------- Counting semaphore implemented via GMutex+GCond -------- */
typedef struct {
  GMutex m;
  GCond c;
  gint count;
  gboolean inited;
} VdSemaphore;

static VdSemaphore discover_sem;
static gint discover_max = 0;

/* ------------------- Semaphore helpers ------------------- */

static void vd_semaphore_init(VdSemaphore *s, gint initial) {
  g_mutex_init(&s->m);
  g_cond_init(&s->c);
  s->count = initial;
  s->inited = TRUE;
}

static void vd_semaphore_acquire(VdSemaphore *s) {
  if (!s->inited)
    return;
  g_mutex_lock(&s->m);
  while (s->count <= 0) {
    g_cond_wait(&s->c, &s->m);
  }
  s->count--;
  g_mutex_unlock(&s->m);
}

static void vd_semaphore_release(VdSemaphore *s) {
  if (!s->inited)
    return;
  g_mutex_lock(&s->m);
  s->count++;
  g_cond_signal(&s->c);
  g_mutex_unlock(&s->m);
}

static void vd_semaphore_clear(VdSemaphore *s) {
  if (!s->inited)
    return;
  g_mutex_clear(&s->m);
  g_cond_clear(&s->c);
  s->count = 0;
  s->inited = FALSE;
}

/* ------------------- Helpers ------------------- */

static gboolean is_video_file(const gchar *name) {
  if (!name)
    return FALSE;
  const gchar *ext = strrchr(name, '.');
  if (!ext)
    return FALSE;
  ext++;

  const gchar *video_exts[] = {
      "mp4", "mkv",  "avi",  "mov", "webm", "flv",  "wmv",  "m4v", "mpeg",
      "mpg", "m2ts", "ts",   "3gp", "ogv",  "mp3",  "flac", "wav", "aac",
      "m4a", "ogg",  "opus", "wma", "alac", "aiff", NULL};

  for (int i = 0; video_exts[i]; i++) {
    if (g_ascii_strcasecmp(ext, video_exts[i]) == 0)
      return TRUE;
  }
  return FALSE;
}

static gchar *format_duration(double seconds) {
  if (seconds <= 0.0)
    return g_strdup("-");
  int total = (int)(seconds + 0.5);
  int h = total / 3600;
  int m = (total % 3600) / 60;
  int s = total % 60;

  /* ✅ 固定宽度 H:MM:SS 便于排序 */
  return g_strdup_printf("%d:%02d:%02d", h, m, s);
}

/* key = path|mtime|size */
static gchar *make_key(const gchar *path, guint64 mtime, guint64 size) {
  return g_strdup_printf("%s|%" G_GUINT64_FORMAT "|%" G_GUINT64_FORMAT, path,
                         mtime, size);
}

/* ------------------- GStreamer Discoverer duration ------------------- */

static void ensure_gst_init_once(void) {
  if (g_once_init_enter(&gst_inited)) {
    gst_init(NULL, NULL);
    g_once_init_leave(&gst_inited, 1);
  }
}

/* timeout seconds, default 3s; override by env NAUTILUS_VD_TIMEOUT */
static guint64 discover_timeout_ns(void) {
  const gchar *env = g_getenv("NAUTILUS_VD_TIMEOUT");
  if (!env || !*env)
    return 3 * GST_SECOND;

  gchar *end = NULL;
  gint v = (gint)g_ascii_strtoll(env, &end, 10);
  if (end == env || v <= 0)
    return 3 * GST_SECOND;

  if (v > 30)
    v = 30;
  return (guint64)v * GST_SECOND;
}

static double get_duration_gst_discoverer(const gchar *path) {
  if (!path)
    return 0.0;

  ensure_gst_init_once();

  gchar *uri = g_filename_to_uri(path, NULL, NULL);
  if (!uri)
    return 0.0;

  GError *err = NULL;
  GstDiscoverer *dc = gst_discoverer_new(discover_timeout_ns(), &err);
  if (!dc) {
    g_message("VD: gst_discoverer_new failed: %s",
              err ? err->message : "(unknown)");
    g_clear_error(&err);
    g_free(uri);
    return 0.0;
  }

  GstDiscovererInfo *info = gst_discoverer_discover_uri(dc, uri, &err);
  if (!info) {
    g_message("VD: discover failed: %s (uri=%s)",
              err ? err->message : "(unknown)", uri);
    g_clear_error(&err);
    g_object_unref(dc);
    g_free(uri);
    return 0.0;
  }

  GstDiscovererResult res = gst_discoverer_info_get_result(info);
  if (res != GST_DISCOVERER_OK) {
    const gchar *msg = (err && err->message) ? err->message : "unknown";
    g_message("VD: discover result=%d err=%s (uri=%s)", (int)res, msg, uri);
    g_clear_error(&err);
    g_object_unref(info);
    g_object_unref(dc);
    g_free(uri);
    return 0.0;
  }

  GstClockTime dur = gst_discoverer_info_get_duration(info);

  g_object_unref(info);
  g_object_unref(dc);
  g_free(uri);

  if (dur == GST_CLOCK_TIME_NONE || dur == 0)
    return 0.0;

  return (double)dur / (double)GST_SECOND;
}

/* ------------------- LRU cache ------------------- */

static void lru_touch_unlocked(gconstpointer key_ptr) {
  GList *node = g_hash_table_lookup(lru_pos, key_ptr);
  if (node) {
    g_queue_unlink(lru, node);
    g_queue_push_tail_link(lru, node);
    return;
  }
  g_queue_push_tail(lru, (gpointer)key_ptr);
  node = g_queue_peek_tail_link(lru);
  g_hash_table_insert(lru_pos, (gpointer)key_ptr, node);
}

static void cache_remove_unlocked(gconstpointer key_ptr) {
  GList *node = g_hash_table_lookup(lru_pos, key_ptr);
  if (node) {
    g_queue_unlink(lru, node);
    g_list_free_1(node);
    g_hash_table_remove(lru_pos, key_ptr);
  }
  g_hash_table_remove(cache, key_ptr); /* frees key/value */
}

static void lru_evict_if_needed_unlocked(void) {
  while ((guint)g_hash_table_size(cache) > cache_max_entries) {
    gpointer old_key_ptr = g_queue_peek_head(lru);
    if (!old_key_ptr)
      break;
    cache_remove_unlocked(old_key_ptr);
  }
}

static void cache_touch_on_hit_unlocked(const gchar *key_tmp) {
  gpointer orig_key = NULL;
  gpointer value = NULL;
  if (g_hash_table_lookup_extended(cache, key_tmp, &orig_key, &value)) {
    lru_touch_unlocked(orig_key);
  }
}

static void cache_put_unlocked(gchar *key_owned, gchar *dur_owned) {
  gpointer orig_key = NULL;
  gpointer value = NULL;
  if (g_hash_table_lookup_extended(cache, key_owned, &orig_key, &value)) {
    cache_remove_unlocked(orig_key);
  }
  g_hash_table_insert(cache, key_owned, dur_owned);
  lru_touch_unlocked(key_owned);
  lru_evict_if_needed_unlocked();
}

/* ------------------- Main thread update ------------------- */

typedef struct {
  NautilusFileInfo *file;
  gchar *dur;
} MainUpdate;

static gboolean apply_update_main(gpointer data) {
  MainUpdate *u = data;

  nautilus_file_info_add_string_attribute(u->file, ATTR_KEY, u->dur);
  nautilus_file_info_invalidate_extension_info(u->file);

  g_object_unref(u->file);
  g_free(u->dur);
  g_free(u);
  return G_SOURCE_REMOVE;
}

static void post_update_to_main(NautilusFileInfo *file, const gchar *dur) {
  if (g_atomic_int_get(&shutting_down))
    return;

  MainUpdate *u = g_new0(MainUpdate, 1);
  u->file = g_object_ref(file);
  u->dur = g_strdup(dur);

  g_main_context_invoke(NULL, apply_update_main, u);
}

/* ------------------- Job ------------------- */

static void job_free(Job *job) {
  if (!job)
    return;
  if (job->file)
    g_object_unref(job->file);
  g_free(job->path);
  g_free(job->key);
  g_free(job);
}

/* ------------------- Config: cache/discover limits ------------------- */

static void init_cache_limit_from_env(void) {
  const gchar *env = g_getenv("NAUTILUS_VD_CACHE");
  if (!env || !*env) {
    cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;
    return;
  }
  gchar *end = NULL;
  guint64 v = g_ascii_strtoull(env, &end, 10);
  if (end == env || v == 0) {
    cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;
    return;
  }
  if (v > 200000)
    v = 200000;
  cache_max_entries = (guint)v;
}

/* Default: min(cores,8). Env: NAUTILUS_VD_DISCOVER */
static gint init_discover_limit_from_env(gint n_threads) {
  gint def = n_threads;
  if (def > 8)
    def = 8;
  if (def < 1)
    def = 1;

  const gchar *env = g_getenv("NAUTILUS_VD_DISCOVER");
  if (!env || !*env)
    return def;

  gchar *end = NULL;
  gint v = (gint)g_ascii_strtoll(env, &end, 10);
  if (end == env)
    return def;

  if (v <= 0)
    return n_threads;
  if (v > n_threads)
    v = n_threads;
  if (v < 1)
    v = 1;
  return v;
}

/* ------------------- Worker ------------------- */

static void worker_func(gpointer data, gpointer user_data) {
  (void)user_data;
  Job *job = data;
  if (!job)
    return;

  if (g_atomic_int_get(&shutting_down))
    goto cleanup;

  vd_semaphore_acquire(&discover_sem);
  double sec = get_duration_gst_discoverer(job->path);
  vd_semaphore_release(&discover_sem);

  gchar *dur = format_duration(sec);

  if (g_atomic_int_get(&shutting_down)) {
    g_free(dur);
    goto cleanup;
  }

  g_mutex_lock(&lock);

  gchar *cached = g_hash_table_lookup(cache, job->key);
  if (!cached) {
    cache_put_unlocked(g_strdup(job->key), g_strdup(dur));
  } else {
    cache_touch_on_hit_unlocked(job->key);
  }

  g_hash_table_remove(inflight, job->key);

  g_mutex_unlock(&lock);

  post_update_to_main(job->file, cached ? cached : dur);
  g_free(dur);

cleanup:
  g_mutex_lock(&lock);
  if (inflight)
    g_hash_table_remove(inflight, job->key);
  g_mutex_unlock(&lock);

  job_free(job);
}

/* ------------------- File stat helper ------------------- */

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

/* ------------------- InfoProvider ------------------- */

static void schedule_duration_job(NautilusFileInfo *file) {
  gchar *name = NULL;
  gchar *uri = NULL;
  gchar *path = NULL;
  gchar *key = NULL;

  guint64 mtime = 0, size = 0;

  if (g_atomic_int_get(&shutting_down))
    return;

  name = nautilus_file_info_get_name(file);
  if (!is_video_file(name))
    goto cleanup;

  uri = nautilus_file_info_get_uri(file);
  if (!uri)
    goto cleanup;

  if (!get_local_path_and_stat(uri, &path, &mtime, &size))
    goto cleanup;

  nautilus_file_info_add_string_attribute(file, ATTR_KEY, "…");
  nautilus_file_info_invalidate_extension_info(file);

  key = make_key(path, mtime, size);

  g_mutex_lock(&lock);

  gchar *cached = g_hash_table_lookup(cache, key);
  if (cached) {
    cache_touch_on_hit_unlocked(key);
    nautilus_file_info_add_string_attribute(file, ATTR_KEY, cached);
    nautilus_file_info_invalidate_extension_info(file);
    g_mutex_unlock(&lock);
    goto cleanup;
  }

  if (g_hash_table_contains(inflight, key)) {
    g_mutex_unlock(&lock);
    goto cleanup;
  }

  g_hash_table_insert(inflight, g_strdup(key), GINT_TO_POINTER(1));
  g_mutex_unlock(&lock);

  Job *job = g_new0(Job, 1);
  job->file = g_object_ref(file);
  job->path = path;
  job->mtime = mtime;
  job->size = size;
  job->key = key;

  path = NULL;
  key = NULL;

  g_thread_pool_push(pool, job, NULL);

cleanup:
  g_free(key);
  g_free(path);
  g_free(uri);
  g_free(name);
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
      nautilus_column_new(COLUMN_ID, ATTR_KEY, "时长", "视频时长");

  return g_list_append(NULL, col);
}

static void column_provider_iface_init(NautilusColumnProviderInterface *iface) {
  iface->get_columns = video_duration_get_columns;
}

/* ------------------- Class init/finalize ------------------- */

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

  g_atomic_int_set(&shutting_down, 0);
  init_cache_limit_from_env();

  g_mutex_init(&lock);

  cache = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
  inflight = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

  lru = g_queue_new();
  lru_pos = g_hash_table_new(g_direct_hash, g_direct_equal);

  gint n_threads = (gint)g_get_num_processors();
  if (n_threads < 1)
    n_threads = 1;

  discover_max = init_discover_limit_from_env(n_threads);
  vd_semaphore_init(&discover_sem, discover_max);

  g_message(
      "VD: threads=%d discover_max=%d cache_max=%u timeout=%" G_GUINT64_FORMAT
      "ns",
      n_threads, discover_max, cache_max_entries, discover_timeout_ns());

  pool = g_thread_pool_new(worker_func, NULL, n_threads, FALSE, NULL);
}

void nautilus_module_shutdown(void) {
  g_atomic_int_set(&shutting_down, 1);

  if (pool) {
    g_thread_pool_free(pool, FALSE, TRUE);
    pool = NULL;
  }

  vd_semaphore_clear(&discover_sem);

  g_mutex_lock(&lock);

  if (inflight) {
    g_hash_table_destroy(inflight);
    inflight = NULL;
  }
  if (lru_pos) {
    g_hash_table_destroy(lru_pos);
    lru_pos = NULL;
  }
  if (lru) {
    g_queue_free(lru);
    lru = NULL;
  }
  if (cache) {
    g_hash_table_destroy(cache);
    cache = NULL;
  }

  g_mutex_unlock(&lock);

  g_mutex_clear(&lock);
}

void nautilus_module_list_types(const GType **types, int *num_types) {
  static GType type_list[1];
  type_list[0] = VIDEO_DURATION_TYPE;
  *types = type_list;
  *num_types = 1;
}
