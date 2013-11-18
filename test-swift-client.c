/*
 * test-swift-client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

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

static enum swift_error
test_swift_client(swift_context_t *swift)
{
	enum swift_error scerr;

	scerr = swift_set_container(swift, SWIFT_CONTAINER);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_create_container(swift, 0, NULL, NULL);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_object(swift, SWIFT_OBJECT);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_put_file(swift, "testdata.txt", 0, NULL, NULL);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_get_file(swift, "testdata2.txt");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_delete_object(swift);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_delete_container(swift);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	return SCERR_SUCCESS;
}

struct keystone_thread_args {
	keystone_context_t *keystone;
	const char *url;
	const char *tenant;
	const char *username;
	const char *password;
	const char *auth_token;
	const char *swift_url;
};

static const char *
keystone_thread_func(void *arg)
{
	struct keystone_thread_args *args = (struct keystone_thread_args *) arg;
	enum keystone_error kserr;

	kserr = keystone_start(args->keystone);
	if (kserr != KSERR_SUCCESS) {
		return NULL;
	}
	pthread_cleanup_push((void (*)(void *)) keystone_end, args->keystone);

	kserr = keystone_set_debug(args->keystone, 1);
	if (KSERR_SUCCESS == kserr) {
		kserr = keystone_set_proxy(args->keystone, PROXY);
	}
	if (KSERR_SUCCESS == kserr) {
		kserr = keystone_authenticate(args->keystone, args->url, args->tenant, args->username, args->password);
	}
	if (KSERR_SUCCESS == kserr) {
		args->auth_token = keystone_get_auth_token(args->keystone);
		args->swift_url = keystone_get_service_url(args->keystone, OS_SERVICE_SWIFT, 0);
	}
	pthread_cleanup_pop(1);

	return NULL;
}

int
main(int argc, char **argv)
{
	swift_context_t swift;
	keystone_context_t keystone;
	enum swift_error scerr;
	enum keystone_error kserr;
	struct keystone_thread_args keystone_args;
	pthread_t keystone_thread;
	pthread_attr_t keystone_thread_attr;
	void *keystone_retval;
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
	keystone_args.url = argv[1];
	keystone_args.tenant = argv[2];
	keystone_args.username = argv[3];
	keystone_args.password = argv[4];

	ret = pthread_attr_init(&keystone_thread_attr);
	if (ret != 0) {
		perror("pthread_attr_init");
		return EXIT_FAILURE;
	}

	ret = pthread_create(&keystone_thread, &keystone_thread_attr, (void *(*)(void *)) keystone_thread_func, &keystone_args);
	if (ret != 0) {
		perror("pthread_create");
		return EXIT_FAILURE;
	}

	ret = pthread_join(keystone_thread, &keystone_retval);
	if (ret != 0) {
		perror("pthread_join");
		return EXIT_FAILURE;
	}

	scerr = swift_start(&swift);
	if (scerr != SCERR_SUCCESS) {
		return EXIT_FAILURE;
	}

	scerr = swift_set_debug(&swift, 1);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	scerr = swift_set_proxy(&swift, PROXY);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	scerr = swift_set_auth_token(&swift, keystone_args.auth_token);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	fprintf(stderr, "Swift is at: %s\n", keystone_args.swift_url);

	scerr = swift_set_url(&swift, keystone_args.swift_url);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	scerr = test_swift_client(&swift);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	swift_end(&swift);

	return EXIT_SUCCESS;
}
