#include <pthread.h> /* pthread_ */
#include <assert.h>  /* assert */
#include <string.h>  /* strdup */

#include "keystone-thread.h"

static void
local_keystone_end(void *arg)
{
	keystone_end((keystone_context_t *) arg);
}

/**
 * Executed by each Keystone thread.
 */
void *
keystone_thread_func(void *arg)
{
	struct keystone_thread_args *args = (struct keystone_thread_args *) arg;

	args->kserr = keystone_start(args->keystone);
	if (KSERR_SUCCESS != args->kserr) {
		return NULL;
	}
	pthread_cleanup_push(local_keystone_end, args->keystone);

	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_set_debug(args->keystone, args->debug);
	}

	if (KSERR_SUCCESS == args->kserr) {
		args->kserr = keystone_set_proxy(args->keystone, args->proxy);
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
