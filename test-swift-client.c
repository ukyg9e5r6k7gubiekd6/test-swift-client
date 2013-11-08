/*
 * test-swift-client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
test_swift_client(swift_context_t *context, const char *keystone_url, const char *tenant_name, const char *username, const char *password)
{
	enum swift_error scerr;

	scerr = swift_set_proxy(context, PROXY);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = keystone_authenticate(context, keystone_url, tenant_name, username, password);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_container(context, SWIFT_CONTAINER);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_create_container(context);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_object(context, SWIFT_OBJECT);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_put_file(context, "testdata.txt", 0, NULL, NULL);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_delete_object(context);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_delete_container(context);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	return SCERR_SUCCESS;
}

int
main(int argc, char **argv)
{
	swift_context_t swift;
	enum swift_error scerr;

	if (argc != 5) {
		fprintf(stderr, USAGE, argv[0]);
		return EXIT_FAILURE;
	}

	memset(&swift, 0, sizeof(swift));

	scerr = swift_global_init();
	if (scerr != SCERR_SUCCESS) {
		return EXIT_FAILURE;
	}

	scerr = swift_start(&swift);
	if (scerr != SCERR_SUCCESS) {
		swift_global_cleanup();
		return EXIT_FAILURE;
	}

	scerr = swift_set_debug(&swift, 1);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		swift_global_cleanup();
		return scerr;
	}

	scerr = test_swift_client(&swift, argv[1], argv[2], argv[3], argv[4]);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		swift_global_cleanup();
		return EXIT_FAILURE;
	}

	swift_end(&swift);
	swift_global_cleanup();

	return EXIT_SUCCESS;
}
