// Copyright (c) 2026 Percona and/or its affiliates.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

// fsfault: an LD_PRELOAD library that makes chosen file system calls fail,
// for testing how the Binlog Server's storage copes with a full disk or I/O
// errors. Linux / glibc only.
//
// Environment:
//   FSFAULT_ROOT   only paths starting with this prefix are affected
//   FSFAULT_RULES  rules separated by ';', each a list of key=value pairs
//                  separated by ',':
//                    op=open|write|fsync|rename|truncate   (required)
//                    match=<substring of the path>          (default: any)
//                    exclude=<substring the path must not contain>
//                    nth=<n>      skip the first n-1 matching calls (1)
//                    count=<n>    fail that many matching calls (1)
//                    errno=ENOSPC|EIO|EROFS|EDQUOT          (ENOSPC)
//                  e.g. "op=write,match=binlog.0,nth=2,count=1,errno=EIO"
//   FSFAULT_LOG    file that gets one line per injected failure
//
// "open" fails opening a file for writing (O_WRONLY / O_RDWR / "w" / "a"),
// "write" covers write / writev / pwrite, "fsync" covers fsync / fdatasync,
// "truncate" covers truncate / ftruncate. Build:
//   gcc -O2 -shared -fPIC -o libfsfault.so fsfault.c -ldl -lpthread

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define MAX_RULES 16
#define MAX_FDS 4096
#define MAX_PATH_LENGTH 1024

enum op_type { OP_OPEN, OP_WRITE, OP_FSYNC, OP_RENAME, OP_TRUNCATE, OP_NONE };

struct rule {
  enum op_type op;
  char match[256];
  char exclude[256];
  long nth;
  long count;
  int error;
  long matched;
};

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static struct rule rules[MAX_RULES];
static int rule_count;
static char root[MAX_PATH_LENGTH];
static char log_path[MAX_PATH_LENGTH];
static char *fd_paths[MAX_FDS];

static int (*real_open)(const char *, int, ...);
static int (*real_open64)(const char *, int, ...);
static int (*real_openat)(int, const char *, int, ...);
static FILE *(*real_fopen)(const char *, const char *);
static FILE *(*real_fopen64)(const char *, const char *);
static int (*real_close)(int);
static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_writev)(int, const struct iovec *, int);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*real_pwrite64)(int, const void *, size_t, off_t);
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static int (*real_rename)(const char *, const char *);
static int (*real_truncate)(const char *, off_t);
static int (*real_truncate64)(const char *, off_t);
static int (*real_ftruncate)(int, off_t);
static int (*real_ftruncate64)(int, off_t);

static enum op_type parse_op(const char *name) {
  if (strcmp(name, "open") == 0) return OP_OPEN;
  if (strcmp(name, "write") == 0) return OP_WRITE;
  if (strcmp(name, "fsync") == 0) return OP_FSYNC;
  if (strcmp(name, "rename") == 0) return OP_RENAME;
  if (strcmp(name, "truncate") == 0) return OP_TRUNCATE;
  return OP_NONE;
}

static int parse_errno(const char *name) {
  if (strcmp(name, "EIO") == 0) return EIO;
  if (strcmp(name, "EROFS") == 0) return EROFS;
  if (strcmp(name, "EDQUOT") == 0) return EDQUOT;
  return ENOSPC;
}

static void initialize(void) {
  real_open = dlsym(RTLD_NEXT, "open");
  real_open64 = dlsym(RTLD_NEXT, "open64");
  real_openat = dlsym(RTLD_NEXT, "openat");
  real_fopen = dlsym(RTLD_NEXT, "fopen");
  real_fopen64 = dlsym(RTLD_NEXT, "fopen64");
  real_close = dlsym(RTLD_NEXT, "close");
  real_write = dlsym(RTLD_NEXT, "write");
  real_writev = dlsym(RTLD_NEXT, "writev");
  real_pwrite = dlsym(RTLD_NEXT, "pwrite");
  real_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
  real_fsync = dlsym(RTLD_NEXT, "fsync");
  real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
  real_rename = dlsym(RTLD_NEXT, "rename");
  real_truncate = dlsym(RTLD_NEXT, "truncate");
  real_truncate64 = dlsym(RTLD_NEXT, "truncate64");
  real_ftruncate = dlsym(RTLD_NEXT, "ftruncate");
  real_ftruncate64 = dlsym(RTLD_NEXT, "ftruncate64");

  const char *root_value = getenv("FSFAULT_ROOT");
  if (root_value != NULL) snprintf(root, sizeof root, "%s", root_value);
  const char *log_value = getenv("FSFAULT_LOG");
  if (log_value != NULL) snprintf(log_path, sizeof log_path, "%s", log_value);

  const char *rules_value = getenv("FSFAULT_RULES");
  if (rules_value == NULL) return;
  char *copy = strdup(rules_value);
  char *rule_save = NULL;
  for (char *rule_text = strtok_r(copy, ";", &rule_save);
       rule_text != NULL && rule_count < MAX_RULES;
       rule_text = strtok_r(NULL, ";", &rule_save)) {
    struct rule parsed = {.op = OP_NONE, .match = "", .exclude = "", .nth = 1,
                          .count = 1, .error = ENOSPC, .matched = 0};
    char *pair_save = NULL;
    for (char *pair = strtok_r(rule_text, ",", &pair_save); pair != NULL;
         pair = strtok_r(NULL, ",", &pair_save)) {
      char *value = strchr(pair, '=');
      if (value == NULL) continue;
      *value++ = '\0';
      if (strcmp(pair, "op") == 0) parsed.op = parse_op(value);
      else if (strcmp(pair, "match") == 0)
        snprintf(parsed.match, sizeof parsed.match, "%s", value);
      else if (strcmp(pair, "exclude") == 0)
        snprintf(parsed.exclude, sizeof parsed.exclude, "%s", value);
      else if (strcmp(pair, "nth") == 0) parsed.nth = atol(value);
      else if (strcmp(pair, "count") == 0) parsed.count = atol(value);
      else if (strcmp(pair, "errno") == 0) parsed.error = parse_errno(value);
    }
    if (parsed.op != OP_NONE) rules[rule_count++] = parsed;
  }
  free(copy);
}

static void log_failure(enum op_type op, const char *path, int error) {
  static const char *names[] = {"open", "write", "fsync", "rename",
                                "truncate"};
  if (log_path[0] == '\0') return;
  // the real calls, so that logging is never itself a fault
  int fd = real_open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd < 0) return;
  char line[MAX_PATH_LENGTH + 64];
  int length = snprintf(line, sizeof line, "%d %s %s %s\n", (int)getpid(),
                        names[op], strerror(error), path);
  if (length > 0) real_write(fd, line, (size_t)length);
  real_close(fd);
}

// whether this call fails, and with which error
static int injected_error(enum op_type op, const char *path) {
  pthread_once(&once, initialize);
  if (path == NULL || rule_count == 0) return 0;
  if (root[0] != '\0' && strncmp(path, root, strlen(root)) != 0) return 0;
  int error = 0;
  pthread_mutex_lock(&lock);
  for (int index = 0; index < rule_count && error == 0; ++index) {
    struct rule *rule = &rules[index];
    if (rule->op != op) continue;
    if (rule->match[0] != '\0' && strstr(path, rule->match) == NULL) continue;
    if (rule->exclude[0] != '\0' && strstr(path, rule->exclude) != NULL)
      continue;
    ++rule->matched;
    if (rule->matched >= rule->nth && rule->matched < rule->nth + rule->count)
      error = rule->error;
  }
  pthread_mutex_unlock(&lock);
  if (error != 0) log_failure(op, path, error);
  return error;
}

static void remember(int fd, const char *path) {
  if (fd < 0 || fd >= MAX_FDS || path == NULL) return;
  pthread_mutex_lock(&lock);
  free(fd_paths[fd]);
  fd_paths[fd] = strdup(path);
  pthread_mutex_unlock(&lock);
}

static int fd_error(enum op_type op, int fd) {
  pthread_once(&once, initialize);
  if (fd < 0 || fd >= MAX_FDS || rule_count == 0) return 0;
  char path[MAX_PATH_LENGTH] = "";
  pthread_mutex_lock(&lock);
  if (fd_paths[fd] != NULL) snprintf(path, sizeof path, "%s", fd_paths[fd]);
  pthread_mutex_unlock(&lock);
  return path[0] == '\0' ? 0 : injected_error(op, path);
}

static int writes(int flags) { return (flags & (O_WRONLY | O_RDWR)) != 0; }

static int open_common(int (*real)(const char *, int, ...), const char *path,
                       int flags, mode_t mode) {
  if (writes(flags)) {
    int error = injected_error(OP_OPEN, path);
    if (error != 0) { errno = error; return -1; }
  }
  int fd = real(path, flags, mode);
  remember(fd, path);
  return fd;
}

int open(const char *path, int flags, ...) {
  pthread_once(&once, initialize);
  mode_t mode = 0;
  if (flags & (O_CREAT | O_TMPFILE)) {
    va_list arguments;
    va_start(arguments, flags);
    mode = (mode_t)va_arg(arguments, int);
    va_end(arguments);
  }
  return open_common(real_open, path, flags, mode);
}

int open64(const char *path, int flags, ...) {
  pthread_once(&once, initialize);
  mode_t mode = 0;
  if (flags & (O_CREAT | O_TMPFILE)) {
    va_list arguments;
    va_start(arguments, flags);
    mode = (mode_t)va_arg(arguments, int);
    va_end(arguments);
  }
  return open_common(real_open64 != NULL ? real_open64 : real_open, path,
                     flags, mode);
}

int openat(int directory, const char *path, int flags, ...) {
  pthread_once(&once, initialize);
  mode_t mode = 0;
  if (flags & (O_CREAT | O_TMPFILE)) {
    va_list arguments;
    va_start(arguments, flags);
    mode = (mode_t)va_arg(arguments, int);
    va_end(arguments);
  }
  // only absolute paths can be matched against the root
  if (path[0] == '/' && writes(flags)) {
    int error = injected_error(OP_OPEN, path);
    if (error != 0) { errno = error; return -1; }
  }
  int fd = real_openat(directory, path, flags, mode);
  if (path[0] == '/') remember(fd, path);
  return fd;
}

static FILE *fopen_common(FILE *(*real)(const char *, const char *),
                          const char *path, const char *mode) {
  if (mode != NULL && (strchr(mode, 'w') || strchr(mode, 'a') ||
                       strchr(mode, '+'))) {
    int error = injected_error(OP_OPEN, path);
    if (error != 0) { errno = error; return NULL; }
  }
  FILE *file = real(path, mode);
  if (file != NULL) remember(fileno(file), path);
  return file;
}

FILE *fopen(const char *path, const char *mode) {
  pthread_once(&once, initialize);
  return fopen_common(real_fopen, path, mode);
}

FILE *fopen64(const char *path, const char *mode) {
  pthread_once(&once, initialize);
  return fopen_common(real_fopen64 != NULL ? real_fopen64 : real_fopen, path,
                      mode);
}

int close(int fd) {
  pthread_once(&once, initialize);
  if (fd >= 0 && fd < MAX_FDS) {
    pthread_mutex_lock(&lock);
    free(fd_paths[fd]);
    fd_paths[fd] = NULL;
    pthread_mutex_unlock(&lock);
  }
  return real_close(fd);
}

ssize_t write(int fd, const void *buffer, size_t size) {
  int error = fd_error(OP_WRITE, fd);
  if (error != 0) { errno = error; return -1; }
  return real_write(fd, buffer, size);
}

ssize_t writev(int fd, const struct iovec *vectors, int count) {
  int error = fd_error(OP_WRITE, fd);
  if (error != 0) { errno = error; return -1; }
  return real_writev(fd, vectors, count);
}

ssize_t pwrite(int fd, const void *buffer, size_t size, off_t offset) {
  int error = fd_error(OP_WRITE, fd);
  if (error != 0) { errno = error; return -1; }
  return real_pwrite(fd, buffer, size, offset);
}

ssize_t pwrite64(int fd, const void *buffer, size_t size, off_t offset) {
  int error = fd_error(OP_WRITE, fd);
  if (error != 0) { errno = error; return -1; }
  return (real_pwrite64 != NULL ? real_pwrite64 : real_pwrite)(fd, buffer,
                                                               size, offset);
}

int fsync(int fd) {
  int error = fd_error(OP_FSYNC, fd);
  if (error != 0) { errno = error; return -1; }
  return real_fsync(fd);
}

int fdatasync(int fd) {
  int error = fd_error(OP_FSYNC, fd);
  if (error != 0) { errno = error; return -1; }
  return real_fdatasync(fd);
}

int rename(const char *from, const char *to) {
  pthread_once(&once, initialize);
  int error = injected_error(OP_RENAME, to);
  if (error != 0) { errno = error; return -1; }
  return real_rename(from, to);
}

int truncate(const char *path, off_t length) {
  pthread_once(&once, initialize);
  int error = injected_error(OP_TRUNCATE, path);
  if (error != 0) { errno = error; return -1; }
  return real_truncate(path, length);
}

int truncate64(const char *path, off_t length) {
  pthread_once(&once, initialize);
  int error = injected_error(OP_TRUNCATE, path);
  if (error != 0) { errno = error; return -1; }
  return (real_truncate64 != NULL ? real_truncate64 : real_truncate)(path,
                                                                     length);
}

int ftruncate(int fd, off_t length) {
  int error = fd_error(OP_TRUNCATE, fd);
  if (error != 0) { errno = error; return -1; }
  return real_ftruncate(fd, length);
}

int ftruncate64(int fd, off_t length) {
  int error = fd_error(OP_TRUNCATE, fd);
  if (error != 0) { errno = error; return -1; }
  return (real_ftruncate64 != NULL ? real_ftruncate64 : real_ftruncate)(
      fd, length);
}
