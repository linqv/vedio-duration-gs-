#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/base64.h>
#include <libavutil/time.h>

typedef struct {
  int64_t deadline_us;
} VdIctx;

static int ff_interrupt_cb(void *opaque) {
  VdIctx *c = (VdIctx *)opaque;
  if (!c)
    return 0;
  return (av_gettime_relative() > c->deadline_us) ? 1 : 0;
}

static uint32_t clamp_u32_from_sec(double sec) {
  if (sec <= 0.0)
    return 0;
  if (sec > (double)UINT32_MAX)
    return UINT32_MAX;
  return (uint32_t)(sec + 0.5);
}

static uint32_t duration_from_fmt_or_streams(AVFormatContext *fmt) {
  if (!fmt)
    return 0;

  if (fmt->duration != AV_NOPTS_VALUE && fmt->duration > 0) {
    return clamp_u32_from_sec((double)fmt->duration / (double)AV_TIME_BASE);
  }

  double best = 0.0;
  for (unsigned i = 0; i < fmt->nb_streams; i++) {
    AVStream *st = fmt->streams[i];
    if (!st)
      continue;
    if (st->duration != AV_NOPTS_VALUE && st->duration > 0) {
      double sec = (double)st->duration * av_q2d(st->time_base);
      if (sec > best)
        best = sec;
    }
  }
  return clamp_u32_from_sec(best);
}

static void set_probe_opts(AVDictionary **d, int pass) {
  if (pass == 0) {
    av_dict_set(d, "probesize", "131072", 0);  /* 128K */
    av_dict_set(d, "analyzeduration", "0", 0); /* 0us */
  } else {
    av_dict_set(d, "probesize", "2097152", 0);       /* 2MB */
    av_dict_set(d, "analyzeduration", "1000000", 0); /* 1s */
  }
  av_dict_set(d, "max_delay", "0", 0);
}

static int run_one_pass(const char *path, int timeout_ms, int pass,
                        uint32_t *out_sec, int *out_timed_out) {
  *out_sec = 0;
  if (out_timed_out)
    *out_timed_out = 0;
  if (!path || timeout_ms <= 0)
    return -1;

  VdIctx ictx;
  ictx.deadline_us = av_gettime_relative() + (int64_t)timeout_ms * 1000;

  AVFormatContext *fmt = avformat_alloc_context();
  if (!fmt)
    return -1;

  fmt->interrupt_callback.callback = ff_interrupt_cb;
  fmt->interrupt_callback.opaque = &ictx;

  AVDictionary *opts = NULL;
  set_probe_opts(&opts, pass);

  int ret = avformat_open_input(&fmt, path, NULL, &opts);
  av_dict_free(&opts);

  if (ret < 0) {
    if (out_timed_out && ff_interrupt_cb(&ictx))
      *out_timed_out = 1;
    avformat_free_context(fmt);
    return ret;
  }

  /* ✅ Fast check */
  uint32_t sec = duration_from_fmt_or_streams(fmt);
  if (sec > 0) {
    *out_sec = sec;
    avformat_close_input(&fmt);
    return 0;
  }

  /* ✅ Key speedup: pass0 does NOT call find_stream_info */
  if (pass == 0) {
    avformat_close_input(&fmt);
    return -1;
  }

  /* pass1 only: bounded stream info */
  AVDictionary *si = NULL;
  set_probe_opts(&si, pass);
  ret = avformat_find_stream_info(fmt, &si);
  av_dict_free(&si);

  if (ret >= 0) {
    sec = duration_from_fmt_or_streams(fmt);
    if (sec > 0) {
      *out_sec = sec;
      avformat_close_input(&fmt);
      return 0;
    }
  }

  if (out_timed_out && ff_interrupt_cb(&ictx))
    *out_timed_out = 1;
  avformat_close_input(&fmt);
  return (ret < 0) ? ret : -1;
}

static void get_duration_adaptive(const char *path, int t1_ms, int t2_ms,
                                  uint32_t *out_sec, int *out_timeout_flag) {
  *out_sec = 0;
  if (out_timeout_flag)
    *out_timeout_flag = 0;

  uint32_t sec = 0;
  int to1 = 0;

  (void)run_one_pass(path, t1_ms, 0, &sec, &to1);
  if (sec > 0) {
    *out_sec = sec;
    return;
  }

  int to2 = 0;
  (void)run_one_pass(path, t2_ms, 1, &sec, &to2);
  if (sec > 0) {
    *out_sec = sec;
    return;
  }

  if (out_timeout_flag)
    *out_timeout_flag = (to1 || to2) ? 1 : 0;
}

static char *read_line(FILE *in) {
  size_t cap = 0;
  char *line = NULL;
  ssize_t n = getline(&line, &cap, in);
  if (n <= 0) {
    free(line);
    return NULL;
  }
  while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
    line[n - 1] = '\0';
    n--;
  }
  return line;
}

static uint8_t *b64_decode_av(const char *b64, int *out_len) {
  *out_len = 0;
  if (!b64)
    return NULL;
  int in_len = (int)strlen(b64);
  int out_cap = (in_len * 3) / 4 + 8;
  uint8_t *buf = (uint8_t *)malloc((size_t)out_cap);
  if (!buf)
    return NULL;
  int n = av_base64_decode(buf, b64, out_cap);
  if (n < 0) {
    free(buf);
    return NULL;
  }
  buf[n] = 0;
  *out_len = n;
  return buf;
}

int main(void) {
  av_log_set_level(AV_LOG_QUIET);
  avformat_network_init();

  while (1) {
    char *line = read_line(stdin);
    if (!line)
      break;

    if (strcmp(line, "QUIT") == 0) {
      free(line);
      break;
    }

    if (strncmp(line, "REQ ", 4) != 0) {
      printf("OK 0 \n");
      fflush(stdout);
      free(line);
      continue;
    }

    char *p = line + 4;
    while (*p == ' ')
      p++;
    char *t1s = p;
    char *sp1 = strchr(t1s, ' ');
    if (!sp1) {
      printf("OK 0 \n");
      fflush(stdout);
      free(line);
      continue;
    }
    *sp1 = '\0';
    p = sp1 + 1;
    while (*p == ' ')
      p++;

    char *t2s = p;
    char *sp2 = strchr(t2s, ' ');
    if (!sp2) {
      printf("OK 0 \n");
      fflush(stdout);
      free(line);
      continue;
    }
    *sp2 = '\0';
    p = sp2 + 1;
    while (*p == ' ')
      p++;

    char *b64 = p;

    int t1 = atoi(t1s);
    int t2 = atoi(t2s);
    if (t1 < 200)
      t1 = 200;
    if (t2 < 500)
      t2 = 500;

    int path_len = 0;
    uint8_t *path_buf = b64_decode_av(b64, &path_len);
    if (!path_buf || path_len <= 0) {
      printf("OK 0 \n");
      fflush(stdout);
      free(path_buf);
      free(line);
      continue;
    }

    uint32_t sec = 0;
    int timed_out = 0;
    get_duration_adaptive((const char *)path_buf, t1, t2, &sec, &timed_out);

    if (timed_out)
      printf("OK %u T\n", sec);
    else
      printf("OK %u \n", sec);
    fflush(stdout);

    free(path_buf);
    free(line);
  }

  avformat_network_deinit();
  return 0;
}
