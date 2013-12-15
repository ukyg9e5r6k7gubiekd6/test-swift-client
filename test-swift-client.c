/*
 * test-swift-client.c
 */

#include <stdio.h>   /* [sw]printf */
#include <stdlib.h>  /* perror, malloc, free */
#include <string.h>  /* memset */
#include <pthread.h> /* pthread_* */
#include <assert.h>  /* assert */
#include <errno.h>   /* errno */

/* If defined, use GNU getopt_long; otherwise, use POSIX getopt */
#define USE_GETOPT_LONG

#ifdef USE_GETOPT_LONG
#include <unistd.h>  /* getopt_long */
#include <getopt.h>  /* getopt_long */
#else /* ndef USE_GETOPT_LONG */
#include <unistd.h>  /* getopt */
#endif /* ndef USE_GETOPT_LONG */

#include "keystone-thread.h"
#include "swift-thread.h"

/* Default number of Swift threads, if not over-ridden on command line */
#define NUM_SWIFT_THREADS_DEFAULT 1
/* Default number of times each thread performs its identical put and its identical get */
#define SWIFT_ITERATIONS_DEFAULT 1
/* Default size in bytes of each Swift object */
#define OBJECT_SIZE_DEFAULT 1024
/* Default type of test data with which to fill Swift objects */
#define OBJECT_DATA_TYPE_DEFAULT SIMPLE_TEXT
/* Default data-verification flag. If true, verify that retrieved data is what was previously inserted. If false, do not perform this verification */
#define VERIFY_DATA_DEFAULT 1

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

static unsigned int
parse_bool(const char * val)
{
	if (
		0 != atoi(val)
		|| 0 == strcasecmp(val, "enabled")
		|| 0 == strcasecmp(val, "on")
		|| 0 == strcasecmp(val, "true")
		|| 0 == strcasecmp(val, "yes")
	) {
		return 1;
	}
	return 0;
}

int
main(int argc, char **argv)
{
	keystone_context_t keystone_context;
	enum keystone_error kserr;
	struct keystone_thread_args keystone_args;
	pthread_t keystone_thread;
	void *keystone_retval;

	swift_context_t *swift_contexts = NULL;
	struct swift_thread_args *swift_args = NULL;
	pthread_t *swift_thread_ids = NULL;
	pthread_cond_t start_condvar = PTHREAD_COND_INITIALIZER;
	pthread_mutex_t start_mutex = PTHREAD_MUTEX_INITIALIZER;
	void **swift_retvals = NULL;

	int ret;
	unsigned int i;

	enum test_data_type data_type = OBJECT_DATA_TYPE_DEFAULT;
	unsigned int iterations = SWIFT_ITERATIONS_DEFAULT;
	const char *keystone_url = NULL;
	unsigned int num_swift_threads = NUM_SWIFT_THREADS_DEFAULT;
	const char *password = NULL;
	const char *proxy = NULL;
	unsigned long object_size = OBJECT_SIZE_DEFAULT;
	const char *tenant_name = NULL;
	const char *username = NULL;
	unsigned int verify_data = VERIFY_DATA_DEFAULT;
	unsigned int verbose = 0;

#define OPTSTRING "d:hi:k:n:p:s:t:u:v:V"
#define HELP "\
Where:\n\
    keystone-endpoint-url\n\
        Is any endpoint URL of the Keystone service;\n\
    password\n\
        Is the password for Keystone authentication;\n\
    tenant-name\n\
        Is the tenant name for Keystone authentication;\n\
    username\n\
        Is the user name for Keystone authentication;\n\
    data\n\
        Is one of:\n\
        random: Fill Swift object(s) with pseudo-random bits;\n\
        simple-text (default): Fill Swift object(s) with identifiable text;\n\
        zeroes: Fill Swift object(s) with zero bits;\n\
    http-proxy\n\
        Is the URL of a proxy to use for access to Keystone and Swift;\n\
    iterations\n\
        Is the number of consecutive gets/puts performed by each Swift thread;\n\
    num-threads\n\
        Is the number of concurrent Swift worker threads;\n\
    size\n\
        Is the size in bytes of each Swift object;\n\
    verify-bool\n\
        Is true if the retrieved objects' data should be compared with\n\
        the data previously inserted into those objects,\n\
        or false if the retrieved objects' data should be thrown away.\n\
"
#ifdef USE_GETOPT_LONG
#define USAGE "\
Usage:\n\
    %s --help\n\
        Outputs this help text\n\
or\n\
    %s\n\
        --keystone-url <keystone-endpoint-URL> --password <password>\n\
        --tenant-name <tenant-name> --username <username> \n\
        [ --data { random | simple-text | zeroes } ]\n\
        [ --http-proxy <proxy-url> ] [ --iterations <n> ]\n\
        [ --num-threads <n> ] [ --size <numbytes> ] [ --verbose ]\n\
        [ --verify-data <verify-bool> ]\n\
\n\
" HELP "\
    verbose\n\
        If supplied, triggers verbose logging of actions performed.\n\
"
	int option_index;
	static struct option long_options[] = {
		{"data",         required_argument, NULL, 'd'},
		{"help",         no_argument,       NULL, 'h'},
		{"http-proxy",   required_argument, NULL, 'r'},
		{"iterations",   required_argument, NULL, 'i'},
		{"keystone-url", required_argument, NULL, 'k'},
		{"num-threads",  required_argument, NULL, 'n'},
		{"password",     required_argument, NULL, 'p'},
		{"size",         required_argument, NULL, 's'},
		{"tenant-name",  required_argument, NULL, 't'},
		{"username",     required_argument, NULL, 'u'},
		{"verbose",      no_argument,       NULL, 'V'},
		{"verify-data",  required_argument, NULL, 'v'},
		{NULL,           0,                 NULL, 0}
	};
#else /* ndef USE_GETOPT_LONG */
#define USAGE "\
Usage:\n\
    %s -h\n\
        Outputs this help text\n\
or\n\
    %s\n\
        -k <keystone-endpoint-URL> -p <password> -t <tenant-name>\n\
        -u <username> [ -d { random | simple-text | zeroes } ]\n\
        [ -i <n> ] [ -n <n> ] [ -r <proxy-url> ] [ -s <numbytes> ]\n\
        [ -v <verify-bool> ] [ -V ]\n\
\n\
" HELP "\
    -V\n\
        If supplied, triggers verbose logging of actions performed.\n\
"
#endif /* USE_GETOPT_LONG */

	for (;;) {
#ifdef USE_GETOPT_LONG
		ret = getopt_long(argc, argv, OPTSTRING, long_options, &option_index);
#else /* ndef USE_GETOPT_LONG */
		ret = getopt(argc, argv, OPTSTRING);
#endif /* ndef USE_GETOPT_LONG */
		if (-1 == ret) {
			break;
		}
		switch (ret) {
		case 'd':
			if (0 == strcmp(optarg, "random")) {
				data_type = PSEUDO_RANDOM;
			} else if (0 == strcmp(optarg, "simple-text")) {
				data_type = SIMPLE_TEXT;
			} else if (0 == strcmp(optarg, "zeroes")) {
				data_type = ALL_ZEROES;
			} else {
				fprintf(stderr, "Unrecognised data type '%s'. Choices are: random, simple-text, zeroes\n", optarg);
				fprintf(stderr, USAGE, argv[0], argv[0]);
				return EXIT_FAILURE;
			}
			break;
		case 'h':
			fprintf(stderr, USAGE, argv[0], argv[0]);
			return EXIT_SUCCESS;
		case 'i':
			iterations = atoi(optarg);
			break;
		case 'k':
			keystone_url = optarg;
			break;
		case 'n':
			num_swift_threads = atoi(optarg);
			break;
		case 'p':
			password = optarg;
			break;
		case 'r':
			proxy = optarg;
			break;
		case 's':
			errno = 0;
			object_size = strtoul(optarg, NULL, 0);
			if (errno) {
				perror("strtoul");
				return EXIT_FAILURE;
			}
			break;
		case 't':
			tenant_name = optarg;
			break;
		case 'u':
			username = optarg;
			break;
		case 'v':
			verify_data = parse_bool(optarg);
			break;
		case 'V':
			verbose = 1;
			break;
		case '?':
		default:
			fprintf(stderr, USAGE, argv[0], argv[0]);
			return EXIT_FAILURE;
		}
	}

	if (optind < argc) {
		fprintf(stderr, "Unrecognised non-option argument: %s\n", argv[optind]);
		fprintf(stderr, USAGE, argv[0], argv[0]);
		return EXIT_FAILURE;
	}

	if (NULL == keystone_url) {
		fputs("Missing required option: "
#ifdef USE_GETOPT_LONG
				"--keystone-url"
#else
				"-k"
#endif
				"\n", stderr);
		fprintf(stderr, USAGE, argv[0], argv[0]);
		return EXIT_FAILURE;
	}

	if (NULL == tenant_name) {
		fputs("Missing required option: "
#ifdef USE_GETOPT_LONG
				"--tenant-name"
#else
				"-t"
#endif
				"\n", stderr);
		fprintf(stderr, USAGE, argv[0], argv[0]);
		return EXIT_FAILURE;
	}

	if (NULL == username) {
		fputs("Missing required option: "
#ifdef USE_GETOPT_LONG
				"--username"
#else
				"-u"
#endif
				"\n", stderr);
		fprintf(stderr, USAGE, argv[0], argv[0]);
		return EXIT_FAILURE;
	}

	if (NULL == password) {
		fputs("Missing required option: "
#ifdef USE_GETOPT_LONG
				"--password"
#else
				"-p"
#endif
				"\n", stderr);
		fprintf(stderr, USAGE, argv[0], argv[0]);
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
	keystone_args.debug = verbose;
	keystone_args.proxy = proxy;
	keystone_args.url = keystone_url;
	keystone_args.tenant = tenant_name;
	keystone_args.username = username;
	keystone_args.password = password;

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
		swift_args[i].debug = verbose;
		swift_args[i].proxy = proxy;
		swift_args[i].thread_num = i;
		swift_args[i].data_type = data_type;
		swift_args[i].data_size = object_size;
		swift_args[i].verify_data = verify_data;
		swift_args[i].num_iterations = iterations;
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
