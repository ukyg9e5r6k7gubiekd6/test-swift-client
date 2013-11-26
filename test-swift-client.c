/*
 * test-swift-client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include "keystone-client.h"
#include "swift-client.h"

#if 0
#define PROXY "socks5://127.0.0.1:8080/"
#else
#define PROXY NULL
#endif
#define NUM_SWIFT_THREADS 10

#define USAGE "Usage: %s <tenant-name> <username> <password>"

#ifdef min
#undef min
#endif
#define min(a, b) ((a) < (b) ? (a) : (b))

#define ELEMENTSOF(arr) ((sizeof(arr) / sizeof((arr)[0])))

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

static void *
keystone_thread_func(void *arg)
{
	struct keystone_thread_args *args = (struct keystone_thread_args *) arg;

	args->kserr = keystone_start(args->keystone);
	if (KSERR_SUCCESS != args->kserr) {
		return NULL;
	}
	pthread_cleanup_push((void (*)(void *)) keystone_end, args->keystone);

	args->kserr = keystone_set_debug(args->keystone, 1);
	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_set_proxy(args->keystone, PROXY);
	}
	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_authenticate(args->keystone, args->url, args->tenant, args->username, args->password);
	}
	if (KSERR_SUCCESS == args->kserr) {
		args->auth_token = strdup(keystone_get_auth_token(args->keystone));
		args->swift_url = strdup(keystone_get_service_url(args->keystone, OS_SERVICE_SWIFT, 0));
	}
	pthread_cleanup_pop(1);

	return NULL;
}

struct compare_data_args {
	swift_context_t *swift;
	void *data;
	size_t len;
	size_t off;
};

static size_t
compare_data(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct compare_data_args *args = (struct compare_data_args *) userdata;

	if (size * nmemb > args->len - args->off) {
		return CURL_READFUNC_ABORT; /* Longer than expected */
	}

	if (memcmp(ptr, (((unsigned char *) args->data)) + args->off, min(size * nmemb, args->len - args->off))) {
		return CURL_READFUNC_ABORT; /* Not the expected data */
	}

	args->off += size * nmemb;

	return size * nmemb;
}

static void
free_test_data(void *arg)
{
	struct compare_data_args *args = (struct compare_data_args *) arg;

	free(args->data);
}

static void
gen_object_name(wchar_t *name, size_t len)
{
	/* FIXME: pthread_t is not necessarily convertible to unsigned long */
	swprintf(name, len, L"Object %ld", (unsigned long) pthread_self());
}

static void
gen_container_name(wchar_t *name, size_t len)
{
	/* FIXME: pthread_t is not necessarily convertible to unsigned long */
	swprintf(name, len, L"Container %ld", (unsigned long) pthread_self());
}

struct swift_thread_args {
	swift_context_t *swift;
	const char *swift_url;
	const char *auth_token;
	enum swift_error scerr;
};

static void *
swift_thread_func(void *arg)
{
	struct swift_thread_args *args;
	struct compare_data_args compare_args;
	/* FIXME: Potential buffer over-runs */
	wchar_t container_name[1024];
	wchar_t object_name[1024];

	assert(arg != NULL);
	args = (struct swift_thread_args *) arg;
	assert(args->swift != NULL);
	assert(args->swift_url != NULL);
	assert(args->auth_token != NULL);

	args->scerr = swift_start(args->swift);
	if (args->scerr != SCERR_SUCCESS) {
		return NULL;
	}
	pthread_cleanup_push((void (*)(void *)) swift_end, args->swift);
	pthread_cleanup_push(free_test_data, &compare_args);

	compare_args.data = args->swift->allocator(NULL, 1024);
	if (NULL == compare_args.data) {
		args->scerr = SCERR_ALLOC_FAILED;
	} else {
		/* FIXME: pthread_t is an opaque type that is not specified to be castable to ulong */
		sprintf(compare_args.data, "This is the test data for thread %ld", (unsigned long) pthread_self());
		compare_args.swift = args->swift;
		compare_args.len = strlen(compare_args.data);
		compare_args.off = 0;
	}

	gen_container_name(container_name, ELEMENTSOF(container_name));
	gen_object_name(object_name, ELEMENTSOF(object_name));

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_debug(args->swift, 1);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_proxy(args->swift, PROXY);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_auth_token(args->swift, args->auth_token);
	}

	if (SCERR_SUCCESS == args->scerr) {
		fprintf(stderr, "Swift is at: %s\n", args->swift_url);
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
		args->scerr = swift_put_data(args->swift, compare_args.data, compare_args.len, 0, NULL, NULL);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_get(args->swift, compare_data, &compare_args);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_delete_object(args->swift);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_delete_container(args->swift);
	}

	pthread_cleanup_pop(1);
	pthread_cleanup_pop(1);

	return NULL;
}

int
main(int argc, char **argv)
{
	keystone_context_t keystone;
	enum keystone_error kserr;
	struct keystone_thread_args keystone_args;
	pthread_t keystone_thread;
	pthread_attr_t keystone_thread_attr;
	void *keystone_retval;

	swift_context_t swift_contexts[NUM_SWIFT_THREADS];
	struct swift_thread_args swift_args[NUM_SWIFT_THREADS];
	pthread_t swift_thread_ids[NUM_SWIFT_THREADS];
	pthread_attr_t swift_thread_attrs;
	void *swift_retvals[NUM_SWIFT_THREADS];

	int ret;
	unsigned int i;

	if (argc != 5) {
		fprintf(stderr, USAGE, argv[0]);
		return EXIT_FAILURE;
	}

	memset(&swift_contexts, 0, sizeof(swift_contexts));
	memset(&keystone, 0, sizeof(keystone));

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
	keystone_args.keystone = &keystone;
	keystone_args.url = argv[1];
	keystone_args.tenant = argv[2];
	keystone_args.username = argv[3];
	keystone_args.password = argv[4];

	ret = pthread_attr_init(&keystone_thread_attr);
	if (ret != 0) {
		perror("pthread_attr_init");
		return EXIT_FAILURE;
	}

	ret = pthread_create(&keystone_thread, &keystone_thread_attr, keystone_thread_func, &keystone_args);
	if (ret != 0) {
		perror("pthread_create");
		return EXIT_FAILURE;
	}

	ret = pthread_join(keystone_thread, &keystone_retval);
	if (ret != 0) {
		perror("pthread_join");
		return EXIT_FAILURE;
	}

	ret = pthread_attr_destroy(&keystone_thread_attr);
	if (ret != 0) {
		perror("pthread_attr_destroy");
		return EXIT_FAILURE;
	}

	if (KSERR_SUCCESS != keystone_args.kserr) {
		return EXIT_FAILURE; /* Keystone thread failed */
	}

	assert(keystone_args.swift_url);
	assert(keystone_args.auth_token);

	memset(&swift_args, 0, sizeof(swift_args));

	ret = pthread_attr_init(&swift_thread_attrs);
	if (ret != 0) {
		perror("pthread_attr_init");
		return EXIT_FAILURE;
	}

	/* Start all of the Swift threads */
	for (i = 0; i < NUM_SWIFT_THREADS; i++) {
		swift_args[i].swift = &swift_contexts[i];
		swift_args[i].swift_url = keystone_args.swift_url;
		swift_args[i].auth_token = keystone_args.auth_token;
		ret = pthread_create(&swift_thread_ids[i], &swift_thread_attrs, swift_thread_func, &swift_args[i]);
		if (ret != 0) {
			perror("pthread_create");
			return EXIT_FAILURE;
		}
	}

	/* Wait for each of the Swift threads to complete */
	for (i = 0; i < NUM_SWIFT_THREADS; i++) {
		ret = pthread_join(swift_thread_ids[i], &swift_retvals[i]);
		if (ret != 0) {
			perror("pthread_join");
			return EXIT_FAILURE;
		}
	}

	ret = pthread_attr_destroy(&swift_thread_attrs);
	if (ret != 0) {
		perror("pthread_attr_destroy");
		return EXIT_FAILURE;
	}

	/* Propagate any error from any of the Swift threads */
	for (i = 0; i < NUM_SWIFT_THREADS; i++) {
		if (SCERR_SUCCESS != swift_args[i].scerr) {
			return EXIT_FAILURE; /* Swift thread failed */
		}
	}

	free(keystone_args.auth_token);
	free(keystone_args.swift_url);

	return SCERR_SUCCESS;
}
