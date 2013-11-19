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
#define SWIFT_ACCOUNT L"testaccount"
#define SWIFT_CONTAINER L"testcontainer"
#define SWIFT_OBJECT L"testobject"

#define USAGE "Usage: %s <tenant-name> <username> <password>"

#ifdef min
#undef min
#endif
#define min(a, b) ((a) < (b) ? (a) : (b))

struct keystone_thread_args {
	keystone_context_t *keystone;
	const char *url;
	const char *tenant;
	const char *username;
	const char *password;
	const char *auth_token;
	const char *swift_url;
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
		args->auth_token = keystone_get_auth_token(args->keystone);
		args->swift_url = keystone_get_service_url(args->keystone, OS_SERVICE_SWIFT, 0);
	}
	pthread_cleanup_pop(1);

	return NULL;
}

struct compare_data_args {
	void *data;
	size_t len;
	size_t off;
};

static size_t
compare_data(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct compare_data_args *args = (struct compare_data_args *) userdata;

	assert(size * nmemb <= args->len - args->off);

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

struct swift_thread_args {
	swift_context_t *swift;
	const char *swift_url;
	const char *auth_token;
	enum swift_error scerr;
};

static void *
swift_thread_func(void *arg)
{
	struct swift_thread_args *args = (struct swift_thread_args *) arg;
	struct compare_data_args compare_args;

	compare_args.data = malloc(1024); /* FIXME: Potential buffer over-run */
	/* FIXME: pthread_t cannot is an opaque type that can't necessarily be converted to ulong */
	sprintf(compare_args.data, "This is the test data for thread %ld", (unsigned long) pthread_self());
	compare_args.len = strlen(compare_args.data);
	compare_args.off = 0;

	args->scerr = swift_start(args->swift);
	if (args->scerr != SCERR_SUCCESS) {
		return NULL;
	}
	pthread_cleanup_push((void (*)(void *)) swift_end, args->swift);
	pthread_cleanup_push(free_test_data, &compare_args);

	args->scerr = swift_set_debug(args->swift, 1);
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
		args->scerr = swift_set_container(args->swift, SWIFT_CONTAINER);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_create_container(args->swift, 0, NULL, NULL);
	}

	if (SCERR_SUCCESS == args->scerr) {
		args->scerr = swift_set_object(args->swift, SWIFT_OBJECT);
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

	swift_context_t swift;
	enum swift_error scerr;
	struct swift_thread_args swift_args;
	pthread_t swift_thread;
	pthread_attr_t swift_thread_attr;
	void *swift_retval;

	int ret;

	if (argc != 5) {
		fprintf(stderr, USAGE, argv[0]);
		return EXIT_FAILURE;
	}

	memset(&swift, 0, sizeof(swift));
	memset(&keystone, 0, sizeof(keystone));

	scerr = swift_global_init();
	if (scerr != SCERR_SUCCESS) {
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
	swift_args.swift = &swift;
	swift_args.swift_url = keystone_args.swift_url;
	swift_args.auth_token = keystone_args.auth_token;

	ret = pthread_attr_init(&swift_thread_attr);
	if (ret != 0) {
		perror("pthread_attr_init");
		return EXIT_FAILURE;
	}

	ret = pthread_create(&swift_thread, &swift_thread_attr, swift_thread_func, &swift_args);
	if (ret != 0) {
		perror("pthread_create");
		return EXIT_FAILURE;
	}

	ret = pthread_join(swift_thread, &swift_retval);
	if (ret != 0) {
		perror("pthread_join");
		return EXIT_FAILURE;
	}

	ret = pthread_attr_destroy(&swift_thread_attr);
	if (ret != 0) {
		perror("pthread_attr_destroy");
		return EXIT_FAILURE;
	}

	if (SCERR_SUCCESS != swift_args.scerr) {
		return EXIT_FAILURE; /* Swift thread failed */
	}

	return SCERR_SUCCESS;
}
