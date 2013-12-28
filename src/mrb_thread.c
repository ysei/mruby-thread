#include <mruby.h>
#include <mruby/string.h>
#include <mruby/array.h>
#include <mruby/hash.h>
#include <mruby/proc.h>
#include <mruby/data.h>
#include <mruby/variable.h>
#include <mruby/thread.h>
#include <string.h>
#ifndef _MSC_VER
#include <strings.h>
#endif
#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#ifdef __unix__
#include <unistd.h>
#include <sys/syscall.h>
#endif
#include <time.h>

typedef struct {
  int argc;
  mrb_value* argv;
  struct RProc* proc;
  pthread_t thread;
  mrb_state* mrb_caller;
  mrb_state* mrb;
  mrb_value result;
  mrb_thread t;
} mrb_thread_context;

#ifdef ENABLE_THREAD

typedef struct mrb_pthread_rwlock_t {
  pthread_rwlock_t lock;
} mrb_pthread_rwlock_t;

int
mrb_gem_rwlock_init(mrb_state *mrb, mrb_rwlock_t *lock)
{
  mrb_pthread_rwlock_t *rwlock = (mrb_pthread_rwlock_t*)mrb_malloc(mrb, sizeof(mrb_pthread_rwlock_t));
  if (lock == NULL) {
    return -1;
  }
  int const ret = pthread_rwlock_init(&rwlock->lock, NULL);
  if (ret != 0) {
    mrb_free(mrb, rwlock);
  } else {
    lock->rwlock = (mrb_gem_rwlock_t)rwlock;
  }
  return ret;
}

int
mrb_gem_rwlock_destroy(mrb_state *mrb, mrb_rwlock_t *lock)
{
  mrb_pthread_rwlock_t *rwlock = (mrb_pthread_rwlock_t*)lock->rwlock;
  if (rwlock == NULL) {
    return RWLOCK_STATUS_INVALID_ARGUMENTS;
  }
  int const ret = pthread_rwlock_destroy(&rwlock->lock);
  if (ret == 0) {
    mrb_free(mrb, rwlock);
    lock->rwlock = NULL;
  }
  return ret;
}

static int
pthread_err2mrb(int error)
{
  switch (error) {
  case 0:
    return RWLOCK_STATUS_OK;
  case ETIMEDOUT:
    return RWLOCK_STATUS_TIMEOUT;
  case EINVAL:
    return RWLOCK_STATUS_INVALID_ARGUMENTS;
  case EDEADLK:
    return RWLOCK_STATUS_ALREADY_LOCKED;
  default:
    return RWLOCK_STATUS_UNKNOWN;
  }
}

int
mrb_gem_rwlock_wrlock(mrb_state *mrb, mrb_rwlock_t *lock, uint32_t timeout_ms)
{
  mrb_pthread_rwlock_t *rwlock = (mrb_pthread_rwlock_t*)lock->rwlock;
  if (!rwlock) {
    return RWLOCK_STATUS_INVALID_ARGUMENTS;
  }
  struct timespec timeout;
  if (clock_gettime(CLOCK_REALTIME, &timeout)) {
    return RWLOCK_STATUS_UNKNOWN;
  }
  timeout.tv_sec  += timeout_ms / 1000;
  timeout.tv_nsec += (timeout_ms % 1000) * 1000000;
  int const error = pthread_rwlock_timedwrlock(&rwlock->lock, &timeout);
  return pthread_err2mrb(error);
}

int
mrb_gem_rwlock_rdlock(mrb_state *mrb, mrb_rwlock_t *lock, uint32_t timeout_ms)
{
  mrb_pthread_rwlock_t *rwlock = (mrb_pthread_rwlock_t*)lock->rwlock;
  if (!rwlock) {
    return RWLOCK_STATUS_INVALID_ARGUMENTS;
  }
  struct timespec timeout;
  if (clock_gettime(CLOCK_REALTIME, &timeout)) {
    return RWLOCK_STATUS_UNKNOWN;
  }
  timeout.tv_sec  += timeout_ms / 1000;
  timeout.tv_nsec += (timeout_ms % 1000) * 1000000;
  int const error = pthread_rwlock_timedrdlock(&rwlock->lock, &timeout);
  return pthread_err2mrb(error);
}

int
mrb_gem_rwlock_unlock(mrb_state *mrb, mrb_rwlock_t *lock)
{
  mrb_pthread_rwlock_t *rwlock = (mrb_pthread_rwlock_t*)lock->rwlock;
  if (!rwlock) {
    return RWLOCK_STATUS_INVALID_ARGUMENTS;
  }
  int const error = pthread_rwlock_unlock(&rwlock->lock);
  return pthread_err2mrb(error);
}

mrb_thread_lock_api const lock_api_entry = {
  mrb_gem_rwlock_init,
  mrb_gem_rwlock_destroy,
  mrb_gem_rwlock_rdlock,
  mrb_gem_rwlock_wrlock,
  mrb_gem_rwlock_unlock,
  NULL,
};

typedef struct mrb_gem_thread_impl_t {
  pthread_t pthread;
} mrb_gem_thread_impl_t;

mrb_gem_thread_t
mrb_gem_thread_get_self(mrb_state *mrb)
{
  mrb_gem_thread_impl_t *impl = (mrb_gem_thread_impl_t*)mrb_malloc(mrb, sizeof(mrb_gem_thread_impl_t));
  if (impl != NULL) {
    impl->pthread = pthread_self();
  }
  return impl;
}

int
mrb_gem_thread_equals(mrb_state *mrb, mrb_gem_thread_t t1, mrb_gem_thread_t t2)
{
  if (t1 == t2) {
    return 1;
  }
  if ((t1 == NULL) || (t2 == NULL)) {
    return 0;
  }
  mrb_gem_thread_impl_t *item1 = (mrb_gem_thread_impl_t*)t1;
  mrb_gem_thread_impl_t *item2 = (mrb_gem_thread_impl_t*)t2;
  return pthread_equal(item1->pthread, item2->pthread) ? 1 : 0;
}

mrb_value
mrb_gem_thread_join(mrb_state *mrb, mrb_gem_thread_t t)
{
  mrb_gem_thread_impl_t *impl = (mrb_gem_thread_impl_t*)t;
  void *ptr;
  pthread_join(impl->pthread, &ptr);
  return mrb_cptr_value(mrb, ptr);
}

void
mrb_gem_thread_free(mrb_state *mrb, mrb_gem_thread_t t)
{
  mrb_free(mrb, t);
}

mrb_thread_api const thread_api_entry = {
  mrb_gem_thread_get_self,
  mrb_gem_thread_equals,
  mrb_gem_thread_join,
  mrb_gem_thread_free,
};

#endif

static void
mrb_thread_context_free(mrb_state *mrb, void *p) {
  mrb_thread_context* context = (mrb_thread_context*) p;
  if (p) {
    if (context->mrb) mrb_close(context->mrb);
    if (context->argv) free(context->argv);
    mrb_free(mrb, p);
  }
}

static const struct mrb_data_type mrb_thread_context_type = {
  "mrb_thread_context", mrb_thread_context_free,
};

// based on https://gist.github.com/3066997
static mrb_value
migrate_simple_value(mrb_state *mrb, mrb_value v, mrb_state *mrb2) {
  mrb_value nv;
  const char *s;
  int len;

  nv.tt = v.tt;
  switch (mrb_type(v)) {
  case MRB_TT_FALSE:
  case MRB_TT_TRUE:
  case MRB_TT_FIXNUM:
    nv.value.i = v.value.i;
    break;
  case MRB_TT_SYMBOL:
    nv = mrb_symbol_value(mrb_intern_str(mrb2, v));
    break;
  case MRB_TT_FLOAT:
    nv.value.f = v.value.f;
    break;
  case MRB_TT_STRING:
    {
      struct RString *str = mrb_str_ptr(v);

      s = str->ptr;
      len = str->len;
      nv = mrb_str_new(mrb2, s, len);
    }
    break;
  case MRB_TT_ARRAY:
    {
      struct RArray *a0, *a1;
      int i;

      a0 = mrb_ary_ptr(v);
      nv = mrb_ary_new_capa(mrb2, a0->len);
      a1 = mrb_ary_ptr(nv);
      for (i=0; i<a0->len; i++) {
        int ai = mrb_gc_arena_save(mrb2);
        a1->ptr[i] = migrate_simple_value(mrb, a0->ptr[i], mrb2);
        a1->len++;
        mrb_gc_arena_restore(mrb2, ai);
      }
    }
    break;
  case MRB_TT_HASH:
    {
      mrb_value ka;
      int i, l;

      nv = mrb_hash_new(mrb2);
      ka = mrb_hash_keys(mrb, v);
      l = RARRAY_LEN(ka);
      for (i = 0; i < l; i++) {
        int ai = mrb_gc_arena_save(mrb2);
        mrb_value k = migrate_simple_value(mrb, mrb_ary_entry(ka, i), mrb2);
        mrb_value o = migrate_simple_value(mrb, mrb_hash_get(mrb, v, k), mrb2);
        mrb_hash_set(mrb2, nv, k, o);
        mrb_gc_arena_restore(mrb2, ai);
      }
    }
    break;
  default:
    mrb_raise(mrb, E_TYPE_ERROR, "cannot migrate object");
    break;
  }
  return nv;
}

static void*
mrb_thread_func(void* data) {
  mrb_thread_context* context = (mrb_thread_context*) data;
  mrb_thread thread;
#ifdef ENABLE_THREAD
  if (FALSE == mrb_vm_attach_thread(context->mrb_caller, &thread)) {
    return NULL;
  }
  mrb_state* mrb = mrb_vm_get_thread_state(thread);
#else
  mrb_state* mrb = mrb_open();
#endif
  context->mrb = mrb;
  context->t = thread;

  mrb_value argv[context->argc];
  int i;

  for (i = 0; i < context->argc; i++) {
    argv[i] = migrate_simple_value(context->mrb_caller, context->argv[i], mrb);
  }
  mrb_free(context->mrb_caller, context->argv);
  context->argv = NULL;

  struct RProc* np = (struct RProc*)mrb_obj_alloc(mrb, MRB_TT_PROC, mrb->proc_class);
  mrb_proc_copy(np, context->proc);
  context->result = mrb_yield_argv(mrb, mrb_obj_value(np), context->argc, argv);
  return NULL;
}

static mrb_value
mrb_thread_init(mrb_state* mrb, mrb_value self) {
  mrb_value proc = mrb_nil_value();
  int argc;
  mrb_value* argv;
  mrb_get_args(mrb, "&*", &proc, &argv, &argc);
  if (!mrb_nil_p(proc)) {
    mrb_thread_context* context = (mrb_thread_context*) mrb_malloc(mrb, sizeof(mrb_thread_context));
    context->mrb_caller = mrb;
    context->mrb = NULL;
    context->proc = mrb_proc_ptr(proc);
    context->argc = argc;
    context->argv = (mrb_value*)mrb_malloc(mrb, sizeof (mrb_value) * context->argc);
    context->result = mrb_nil_value();

    int i;
    for (i = 0; i < argc; ++i) {
      context->argv[i] = argv[i];
    }

    mrb_iv_set(mrb, self, mrb_intern_cstr(mrb, "context"), mrb_obj_value(
      Data_Wrap_Struct(mrb, mrb->object_class,
      &mrb_thread_context_type, (void*) context)));

    pthread_create(&context->thread, NULL, &mrb_thread_func, (void*) context);
  }
  return self;
}

static mrb_value
mrb_thread_join(mrb_state* mrb, mrb_value self) {
  mrb_value value_context = mrb_iv_get(mrb, self, mrb_intern_cstr(mrb, "context"));
  mrb_thread_context* context = NULL;
  Data_Get_Struct(mrb, value_context, &mrb_thread_context_type, context);
  pthread_join(context->thread, NULL);

  int const arena_size = mrb_gc_arena_save(mrb);
  mrb_gc_protect(mrb, context->result);
  mrb_gc_arena_restore(mrb, arena_size);
#ifdef ENABLE_THREAD
  mrb_vm_detach_thread(mrb, context->t);
#else
  mrb_close(context->mrb);
#endif
  context->mrb = NULL;
  return context->result;
}

void
mrb_mruby_thread_gem_init(mrb_state* mrb) {
  struct RClass* _class_thread = mrb_define_class(mrb, "Thread", mrb->object_class);
  mrb_define_method(mrb, _class_thread, "initialize", mrb_thread_init, ARGS_OPT(1));
  mrb_define_method(mrb, _class_thread, "join", mrb_thread_join, ARGS_NONE());

#ifdef ENABLE_THREAD
  mrb_vm_thread_api_set(mrb, &thread_api_entry);
  mrb_vm_lock_api_set(mrb, &lock_api_entry);
#endif
}

void
mrb_mruby_thread_gem_final(mrb_state* mrb) {
}

/* vim:set et ts=2 sts=2 sw=2 tw=0: */
