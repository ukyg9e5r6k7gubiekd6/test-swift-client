/*
 * test-swift-client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "swift-client.h"

static size_t
supply_data_from_file(void *ptr, size_t size, size_t nmemb, void *stream)
{
	return fread(ptr, size, nmemb, stream);
}

static enum swift_error
test_swift_client(swift_context_t *context)
{
	enum swift_error scerr;
	CURLcode curl_err;
	FILE *stream;

	scerr = swift_set_hostname(context, "localhost");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_auth_token(context, "myauthtoken");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_account(context, L"myaccount");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_container(context, L"mycontainer");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	scerr = swift_set_object(context, L"myobject");
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	stream = fopen("testdata.txt", "rb");
	if (NULL == stream) {
		perror("fopen");
		return SCERR_FILEIO_FAILED;
	}

	curl_err = curl_easy_setopt(context->pvt.curl, CURLOPT_READDATA, stream);
	if (CURLE_OK != curl_err) {
		return SCERR_FILEIO_FAILED;
	}

	scerr = swift_put(context, supply_data_from_file, 0, NULL, NULL);
	if (scerr != SCERR_SUCCESS) {
		return scerr;
	}

	fclose(stream);

	return SCERR_SUCCESS;
}

int
main(int argc, char **argv)
{
	swift_context_t swift;
	enum swift_error scerr;

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

	scerr = test_swift_client(&swift);
	if (scerr != SCERR_SUCCESS) {
		swift_end(&swift);
		swift_global_cleanup();
		return EXIT_FAILURE;
	}

	swift_end(&swift);
	swift_global_cleanup();

	return EXIT_SUCCESS;
}
