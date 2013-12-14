/*
 * test-swift-client.c
 */

#include <stdio.h>   /* [sw]printf */
#include <stdlib.h>  /* perror, malloc, free, strdup */
#include <string.h>  /* strlen */
#include <pthread.h> /* pthread_ */
#include <assert.h>  /* assert */
#include <time.h>    /* clock_gettime */

#include "keystone-client.h"
#include "swift-client.h"

#define USAGE "Usage: %s <keystone-URL> <tenant-name> <username> <password> [ <num-threads> = 1 ]"

#if 0
#define PROXY "socks5://127.0.0.1:8080/"
#else
#define PROXY NULL
#endif
/* Default number of Swift threads, if not over-ridden on command line */
#define NUM_SWIFT_THREADS_DEFAULT 1
/* Number of times each thread performs its identical put and its identical get */
/* TODO: Make this a default, and accept a command-line argument to over-ride it */
#define NUM_SWIFT_ITERATIONS 1
/* Size of Swift objects */
/* TODO: Make this a default, and accept a command-line argument to over-ride it */
#define OBJECT_SIZE 1024
/* Type of test data with which to fill Swift objects */
/* TODO: Make this a default, and accept a command-line argument to over-ride it */
#define OBJECT_DATA SIMPLE_TEXT
/* If true, verify that retrieved data is what was previously inserted. If false, do not perform this verification */
/* TODO: Make this a default, and accept a command-line argument to over-ride it */
#define VERIFY_RETRIEVED_DATA 1
/* File from which to read pseudo-random data */
#define RANDOM_FILE "/dev/urandom"

/* #define DEBUG_CURL */

#ifdef min
#undef min
#endif
#define min(a, b) ((a) < (b) ? (a) : (b))

#define ELEMENTSOF(arr) ((sizeof(arr) / sizeof((arr)[0])))
#define typealloc(type) (((type) *) malloc(sizeof(type)))
#define typearrayalloc(count, type) ((type *) malloc((count) * sizeof(type)))

#ifdef CLOCK_MONOTONIC_RAW
/* Use NTP-immune but Linux-specific clock */
#define CLOCK_TO_USE CLOCK_MONOTONIC_RAW
#else /* ndef CLOCK_MONOTONIC_RAW */
/* Use POSIX-defined but NTP-vulnerable clock */
#define CLOCK_TO_USE CLOCK_MONOTONIC
#endif /* ndef CLOCK_MONOTONIC_RAW */

/* In/out parameters to a Keystone thread */
struct keystone_thread_args {
	keystone_context_t *keystone;
	const char *url;
	const char *tenant;
	const char *username;
	const char *password;
	char *auth_token;
	char *swift_url;
	enum keystone_error kserr;
};

static void
local_keystone_end(void *arg)
{
	keystone_end((keystone_context_t *) arg);
}

/**
 * Executed by each Keystone thread.
 */
static void *
keystone_thread_func(void *arg)
{
	struct keystone_thread_args *args = (struct keystone_thread_args *) arg;

	args->kserr = keystone_start(args->keystone);
	if (KSERR_SUCCESS != args->kserr) {
		return NULL;
	}
	pthread_cleanup_push(local_keystone_end, args->keystone);

#ifdef DEBUG_CURL
	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_set_debug(args->keystone, 1);
	}
#endif /* DEBUG_CURL */

	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_set_proxy(args->keystone, PROXY);
	}

	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_authenticate(args->keystone, args->url, args->tenant, args->username, args->password);
	}

	if (KSERR_SUCCESS == args->kserr) {
		const char *auth_token = keystone_get_auth_token(args->keystone);
		if (auth_token) {
			args->auth_token = strdup(auth_token);
		} else {
			args->kserr = KSERR_AUTH_REJECTED;
		}
	}

	if (KSERR_SUCCESS == args->kserr) {
		const char *swift_url = keystone_get_service_url(args->keystone, OS_SERVICE_SWIFT, 0, OS_ENDPOINT_URL_PUBLIC);
		if (swift_url) {
			args->swift_url = strdup(swift_url);
		} else {
			args->kserr = KSERR_INIT_FAILED; /* Not the right error code, but Keystone should not know about failure to find Swift */
		}
	}

	pthread_cleanup_pop(1);

	return NULL;
}

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

/* Types of test data with which to pupulate the Swift object */
enum test_data_type {
	SIMPLE_TEXT,  /* Simple text, easily identifiable in the Swift object's data */
	ALL_ZEROS,    /* Null bytes */
	PSEUDO_RANDOM /* Pseudo-random bits */
};

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
 * Generate a Swift conainer name unique to this thread.
 */
static void
gen_container_name(unsigned int thread_num, wchar_t *name, size_t len)
{
	swprintf(name, len, L"Container %u", thread_num);
}

/**
 * In/out parameters to a Swift thread.
 */
struct swift_thread_args {
	swift_context_t *swift;
	unsigned int thread_num;
	const char *swift_url;
	const char *auth_token;
	enum swift_error scerr;
	pthread_cond_t start_condvar;
	pthread_mutex_t start_mutex;
	enum test_data_type data_type;
	size_t data_size;
	unsigned int num_iterations;
	unsigned int verify_data;
	struct timespec start_time;
	struct timespec start_put_time;
	struct timespec end_put_time;
	struct timespec start_get_time;
	struct timespec end_get_time;
	struct timespec end_time;
};

static void
local_swift_end(void *arg)
{
	swift_end((swift_context_t *) arg);
}

/**
 * Executed by each Swift thread.
 */
static void *
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

#ifdef DEBUG_CURL
	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_debug(args->swift, 1);
	}
#endif /* DEBUG_CURL */

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_proxy(args->swift, PROXY);
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

static double
timespecs_to_microsecs(const struct timespec *start, const struct timespec *end)
{
	double d = end->tv_sec - start->tv_sec;
	return d * 1000000 + (end->tv_nsec - start->tv_nsec) / 1000;
}

/**
 * Display the execution time of each of the Swift threads in microseconds.
 */
static void
show_swift_times(const struct swift_thread_args *args, unsigned int n)
{
	fprintf(stderr, "Swift execution times for %u threads:\n", n);
	while (n--) {
		fprintf(stderr, "Thread %3u: total duration (microseconds): %8.4f\n", args->thread_num, timespecs_to_microsecs(&args->start_time, &args->end_time));
		fprintf(stderr, "Thread %3u:   put duration (microseconds): %8.4f\n", args->thread_num, timespecs_to_microsecs(&args->start_put_time, &args->end_put_time));
		fprintf(stderr, "Thread %3u:   get duration (microseconds): %8.4f\n", args->thread_num, timespecs_to_microsecs(&args->start_get_time, &args->end_get_time));
		args++;
	}
}

int
main(int argc, char **argv)
{
	keystone_context_t keystone_context;
	enum keystone_error kserr;
	struct keystone_thread_args keystone_args;
	pthread_t keystone_thread;
	void *keystone_retval;
	unsigned int num_swift_threads;

	swift_context_t *swift_contexts = NULL;
	struct swift_thread_args *swift_args = NULL;
	pthread_t *swift_thread_ids = NULL;
	pthread_cond_t start_condvar = PTHREAD_COND_INITIALIZER;
	pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
	void **swift_retvals = NULL;

	int ret;
	unsigned int i;

	/* TODO: Use getopt */
	if (5 == argc) {
		num_swift_threads = NUM_SWIFT_THREADS_DEFAULT;
	} else if (6 == argc) {
		num_swift_threads = atoi(argv[5]);
		if (0 == num_swift_threads) {
			fputs("Swift thread count must be non-zero\n", stderr);
			fprintf(stderr, USAGE, argv[0]);
			return EXIT_FAILURE;
		}
	} else {
		fprintf(stderr, USAGE, argv[0]);
		return EXIT_FAILURE;
	}

	swift_contexts = typearrayalloc(num_swift_threads, swift_context_t);
	if (NULL == swift_contexts) {
		return EXIT_FAILURE;
	}
	swift_args = typearrayalloc(num_swift_threads, struct swift_thread_args);
	if (NULL == swift_args) {
		return EXIT_FAILURE;
	}
	swift_thread_ids = typearrayalloc(num_swift_threads, pthread_t);
	if (NULL == swift_thread_ids) {
		return EXIT_FAILURE;
	}
	swift_retvals = typearrayalloc(num_swift_threads, void *);
	if (NULL == swift_retvals) {
		return EXIT_FAILURE;
	}

	memset(&swift_contexts, 0, num_swift_threads * sizeof(swift_context_t));
	memset(&keystone_context, 0, sizeof(keystone_context));

	if (swift_global_init() != SCERR_SUCCESS) {
		return EXIT_FAILURE;
	}
	atexit(swift_global_cleanup);

	kserr = keystone_global_init();
	if (kserr != KSERR_SUCCESS) {
		return EXIT_FAILURE;
	}
	atexit(keystone_global_cleanup);

	memset(&keystone_args, 0, sizeof(keystone_args));
	keystone_args.keystone = &keystone_context;
	keystone_args.url = argv[1];
	keystone_args.tenant = argv[2];
	keystone_args.username = argv[3];
	keystone_args.password = argv[4];

	ret = pthread_create(&keystone_thread, NULL, keystone_thread_func, &keystone_args);
	if (ret != 0) {
		perror("pthread_create");
		return EXIT_FAILURE;
	}

	ret = pthread_join(keystone_thread, &keystone_retval);
	if (ret != 0) {
		perror("pthread_join");
		return EXIT_FAILURE;
	}

	if (KSERR_SUCCESS != keystone_args.kserr) {
		return EXIT_FAILURE; /* Keystone thread failed */
	}

	assert(keystone_args.swift_url);
	assert(keystone_args.auth_token);

	memset(&swift_args, 0, sizeof(swift_args));

	/* Start all of the Swift threads */
	for (i = 0; i < num_swift_threads; i++) {
		swift_args[i].swift = &swift_contexts[i];
		swift_args[i].thread_num = i;
		swift_args[i].data_type = OBJECT_DATA;
		swift_args[i].data_size = OBJECT_SIZE;
		swift_args[i].verify_data = VERIFY_RETRIEVED_DATA;
		swift_args[i].num_iterations = NUM_SWIFT_ITERATIONS;
		swift_args[i].swift_url = keystone_args.swift_url;
		swift_args[i].auth_token = keystone_args.auth_token;
		swift_args[i].start_condvar = start_condvar;
		swift_args[i].start_mutex = start_mutex;
		ret = pthread_create(&swift_thread_ids[i], NULL, swift_thread_func, &swift_args[i]);
		if (ret != 0) {
			perror("pthread_create");
			return EXIT_FAILURE;
		}
	}

	/* Broadcast the start condvar, telling all Swift threads to start */
	ret = pthread_mutex_lock(&start_mutex);
	if (ret != 0) {
		perror("pthread_mutex_lock");
		return EXIT_FAILURE;
	}

	ret = pthread_cond_broadcast(&start_condvar);
	if (ret != 0) {
		perror("pthread_cond_broadcast");
		return EXIT_FAILURE;
	}

	ret = pthread_mutex_unlock(&start_mutex);
	if (ret != 0) {
		perror("pthread_mutex_unlock");
		return EXIT_FAILURE;
	}

	/* Wait for each of the Swift threads to complete */
	for (i = 0; i < num_swift_threads; i++) {
		ret = pthread_join(swift_thread_ids[i], &swift_retvals[i]);
		if (ret != 0) {
			perror("pthread_join");
			return EXIT_FAILURE;
		}
	}

	ret = pthread_cond_destroy(&start_condvar);
	if (ret != 0) {
		perror("pthread_cond_destroy");
		return EXIT_FAILURE;
	}

	ret = pthread_mutex_destroy(&start_mutex);
	if (ret != 0) {
		perror("pthread_mutex_destroy");
		return EXIT_FAILURE;
	}

	show_swift_times(swift_args, num_swift_threads);

	ret = SCERR_SUCCESS;
	/* Propagate any error from any of the Swift threads */
	for (i = 0; i < num_swift_threads; i++) {
		if (SCERR_SUCCESS != swift_args[i].scerr) {
			ret = EXIT_FAILURE; /* Swift thread failed */
		}
	}

	free(keystone_args.auth_token);
	free(keystone_args.swift_url);
	free(swift_contexts);
	free(swift_args);
	free(swift_thread_ids);
	free(swift_retvals);

	return ret;
}
