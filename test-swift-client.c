/*
 * test-swift-client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

int
main(int argc, char **argv)
{
	swift_context_t swift;
	keystone_context_t keystone;
	enum swift_error scerr;
	enum keystone_error kserr;
	const char *swift_url;

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

	scerr = swift_start(&swift);
	if (scerr != SCERR_SUCCESS) {
		return EXIT_FAILURE;
	}

	kserr = keystone_start(&keystone);
	if (kserr != KSERR_SUCCESS) {
		swift_end(&swift);
		return EXIT_FAILURE;
	}

	scerr = swift_set_debug(&swift, 1);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	kserr = keystone_set_debug(&keystone, 1);
	if (kserr != KSERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	scerr = swift_set_proxy(&swift, PROXY);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	kserr = keystone_set_proxy(&keystone, PROXY);
	if (kserr != KSERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	kserr = keystone_authenticate(&keystone, argv[1], argv[2], argv[3], argv[4]);
	if (kserr != KSERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	scerr = swift_set_auth_token(&swift, keystone_get_auth_token(&keystone));
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	swift_url = keystone_get_service_url(&keystone, OS_SERVICE_SWIFT, 0);
	fprintf(stderr, "Swift is at: %s\n", swift_url);

	scerr = swift_set_url(&swift, swift_url);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	scerr = test_swift_client(&swift);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		keystone_end(&keystone);
		return EXIT_FAILURE;
	}

	swift_end(&swift);
	keystone_end(&keystone);

	return EXIT_SUCCESS;
}
