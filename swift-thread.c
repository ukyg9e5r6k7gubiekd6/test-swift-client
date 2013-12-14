#include <stdio.h>   /* [sw]printf */
#include <stdlib.h>  /* perror, malloc, free, strdup */
#include <string.h>  /* strdup */
#include <pthread.h> /* pthread_* */
#include <assert.h>  /* assert */
#include <time.h>    /* clock_gettime */

#include "swift-thread.h"

#ifdef CLOCK_MONOTONIC_RAW
/* Use NTP-immune but Linux-specific clock */
#define CLOCK_TO_USE CLOCK_MONOTONIC_RAW
#else /* ndef CLOCK_MONOTONIC_RAW */
/* Use POSIX-defined but NTP-vulnerable clock */
#define CLOCK_TO_USE CLOCK_MONOTONIC
#endif /* ndef CLOCK_MONOTONIC_RAW */

/* File from which to read pseudo-random data */
#define RANDOM_FILE "/dev/urandom"

#ifdef min
#undef min
#endif
#define min(a, b) ((a) < (b) ? (a) : (b))

#define ELEMENTSOF(arr) ((sizeof(arr) / sizeof((arr)[0])))

/* In/out arguments to a compare_data callback */
struct compare_data_args {
	swift_context_t *swift;
	void *data;
	size_t len;
	size_t off;
};

/**
 * Compare the given data to that expected.
 */
static size_t
compare_data(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct compare_data_args *args = (struct compare_data_args *) userdata;

	if (size * nmemb > args->len - args->off) {
		return CURL_READFUNC_ABORT; /* Longer than expected */
	}

	if (NULL == args->data) {
		/* Require received data to be all-zero bytes */
		const char *p;
		for (p = ptr; p < (char *) ptr + (size * nmemb); p++) {
			if (*p) {
				return CURL_READFUNC_ABORT; /* Not the expected data */
			}
		}
	} else {
		/* Require received data to be identical to expected data */
		if (memcmp(ptr, (((unsigned char *) args->data)) + args->off, min(size * nmemb, args->len - args->off))) {
			return CURL_READFUNC_ABORT; /* Not the expected data */
		}
	}

	args->off += size * nmemb;

	return size * nmemb;
}

/**
 * Ignore the given data.
 */
static size_t
ignore_data(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	return size * nmemb;
}

/**
 * Supply zeroed data on request.
 */
static size_t
make_zero_data(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	size *= nmemb;
	memset(ptr, 0, size);
	return size;
}

/**
 * Fill the given test data with repetitions of an easily-identifiable text.
 */
static void
gen_test_data_simple_text(unsigned int thread_num, char *data, size_t len)
{
	const char *chunk_fmt = "This is the test data for thread %u ";
	size_t chunk_len = snprintf(NULL, 0, chunk_fmt, thread_num);
	char *p;

	for (p = data; len > chunk_len; p += chunk_len, len -= chunk_len) {
		sprintf(p, chunk_fmt, thread_num);
	}
}

/**
 * Fill the given test data with pseudo-random bits.
 */
static void
gen_test_data_urandom(char *data, size_t len)
{
	int ret;
	size_t size;
	FILE *urandom;

	urandom = fopen(RANDOM_FILE, "r");
	if (NULL == urandom) {
		perror("fopen " RANDOM_FILE);
		exit(EXIT_FAILURE);
	}
	size = fread(data, 1, len, urandom);
	if (size != len) {
		perror("fread " RANDOM_FILE);
		exit(EXIT_FAILURE);
	}
	ret = fclose(urandom);
	if (ret != 0) {
		perror("fclose " RANDOM_FILE);
		exit(EXIT_FAILURE);
	}
}

/**
 * Fill the given test data with the given type of data.
 */
static void
gen_test_data(unsigned int thread_num, enum test_data_type data_type, char *data, size_t len)
{
	switch(data_type) {
	case SIMPLE_TEXT:
		gen_test_data_simple_text(thread_num, data, len);
		break;
	case ALL_ZEROS:
		/* Nothing to do */
		break;
	case PSEUDO_RANDOM:
		gen_test_data_urandom(data, len);
		break;
	default:
		assert(0);
		break;
	}
}

static void
free_test_data(void *arg)
{
	struct compare_data_args *args = (struct compare_data_args *) arg;

	if (args->data) {
		free(args->data);
	}
}

/**
 * Generate a Swift object name unique to this thread.
 */
static void
gen_object_name(unsigned int thread_num, wchar_t *name, size_t len)
{
	swprintf(name, len, L"Object %u", thread_num);
}

/**
 * Generate a Swift container name unique to this thread.
 */
static void
gen_container_name(unsigned int thread_num, wchar_t *name, size_t len)
{
	swprintf(name, len, L"Container %u", thread_num);
}

static void
local_swift_end(void *arg)
{
	swift_end((swift_context_t *) arg);
}

/**
 * Executed by each Swift thread.
 */
void *
swift_thread_func(void *arg)
{
	struct swift_thread_args *args;
	struct compare_data_args compare_args;
	/* FIXME: Potential buffer over-runs */
	wchar_t container_name[1024];
	wchar_t object_name[1024];
	int ret;

	assert(arg != NULL);
	args = (struct swift_thread_args *) arg;
	assert(args->swift != NULL);
	assert(args->swift_url != NULL);
	assert(args->auth_token != NULL);

	args->scerr = swift_start(args->swift);
	if (args->scerr != SCERR_SUCCESS) {
		return NULL;
	}
	pthread_cleanup_push(local_swift_end, args->swift);

	if (SCERR_SUCCESS == args->scerr) {
		/* Save thread start time */
		ret = clock_gettime(CLOCK_TO_USE, &args->start_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	compare_args.swift = args->swift;
	compare_args.off = 0;
	compare_args.len = args->data_size;
	if (ALL_ZEROS == args->data_type) {
		/* Special case: there is no need ever to actually store a large number of zero bits */
		compare_args.data = NULL;
	} else {
		compare_args.data = args->swift->allocator(NULL, args->data_size);
		if (NULL == compare_args.data) {
			args->scerr = SCERR_ALLOC_FAILED;
		} else {
			gen_test_data(args->thread_num, args->data_type, compare_args.data, compare_args.len);
		}
	}
	pthread_cleanup_push(free_test_data, &compare_args);

	gen_container_name(args->thread_num, container_name, ELEMENTSOF(container_name));
	gen_object_name(args->thread_num, object_name, ELEMENTSOF(object_name));

	if (SCERR_SUCCESS == args->scerr) {
		if (args->debug) {
			args->scerr = swift_set_debug(args->swift, 1);
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_proxy(args->swift, args->proxy);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_auth_token(args->swift, args->auth_token);
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* fprintf(stderr, "Swift is at: %s\n", args->swift_url); */
		args->scerr = swift_set_url(args->swift, args->swift_url);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_container(args->swift, container_name);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_create_container(args->swift, 0, NULL, NULL);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_object(args->swift, object_name);
	}

	if (SCERR_SUCCESS == args->scerr) {
		ret = pthread_mutex_lock(&args->start_mutex);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about pthread mutex errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		ret = pthread_cond_wait(&args->start_condvar, &args->start_mutex);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about pthread condvar errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		ret = pthread_mutex_unlock(&args->start_mutex);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about pthread mutex errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* Save time at start of put operations */
		ret = clock_gettime(CLOCK_TO_USE, &args->start_put_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		unsigned int i;
		for (i = 0; i < args->num_iterations; i++) {
			if (NULL == compare_args.data) {
				/* Special case for all-zero data: Synthesise the data to be inserted at this point */
				args->scerr = swift_put(args->swift, make_zero_data, NULL, 0, NULL, NULL);
			} else {
				args->scerr = swift_put_data(args->swift, compare_args.data, compare_args.len, 0, NULL, NULL);
			}
			if (args->scerr != SCERR_SUCCESS) {
				break;
			}
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* Save time at end of put operations */
		ret = clock_gettime(CLOCK_TO_USE, &args->end_put_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* Save time at start of get operations */
		ret = clock_gettime(CLOCK_TO_USE, &args->start_get_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		unsigned int i;
		for (i = 0; i < args->num_iterations; i++) {
			if (args->verify_data) {
				args->scerr = swift_get(args->swift, compare_data, &compare_args);
			} else {
				args->scerr = swift_get(args->swift, ignore_data, NULL);
			}
			if (args->scerr != SCERR_SUCCESS) {
				break;
			}
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* Save time at end of get operations */
		ret = clock_gettime(CLOCK_TO_USE, &args->end_get_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_delete_object(args->swift);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_delete_container(args->swift);
	}

	if (SCERR_SUCCESS == args->scerr) {
		/* Save end time */
		ret = clock_gettime(CLOCK_TO_USE, &args->end_time);
		if (ret != 0) {
			args->scerr = SCERR_INIT_FAILED; /* Not the right error code, but swift client should not know about POSIX clock errors */
		}
	}

	pthread_cleanup_pop(1);
	pthread_cleanup_pop(1);

	return NULL;
}
