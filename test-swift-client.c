/*
 * test-swift-client.c
 */

#include <stdio.h>   /* [sw]printf */
#include <stdlib.h>  /* perror, malloc, free */
#include <string.h>  /* memset */
#include <pthread.h> /* pthread_* */
#include <assert.h>  /* assert */

#include "keystone-thread.h"
#include "swift-thread.h"

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

/* #define DEBUG_CURL */

#define typealloc(type) (((type) *) malloc(sizeof(type)))
#define typearrayalloc(count, type) ((type *) malloc((count) * sizeof(type)))

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
#ifdef DEBUG_CURL
	keystone_args.debug = 1;
#else /* ndef DEBUG_CURL */
	keystone_args.debug = 0;
#endif /* ndef DEBUG_CURL */
	keystone_args.proxy = PROXY;
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
#ifdef DEBUG_CURL
		swift_args[i].debug = 1;
#else /* ndef DEBUG_CURL */
		swift_args[i].debug = 0;
#endif /* ndef DEBUG_CURL */
		swift_args[i].proxy = PROXY;
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
