#ifndef KEYSTONE_THREAD_H_
#define KEYSTONE_THREAD_H_

#include "keystone-client.h"

/* In/out parameters to a Keystone thread */
struct keystone_thread_args {
	keystone_context_t *keystone;
	const char *proxy;
	const char *url;
	const char *tenant;
	const char *username;
	const char *password;
	char *auth_token;
	char *swift_url;
	enum keystone_error kserr;
};

void *keystone_thread_func(void *arg);

#endif /* KEYSTONE_THREAD_H_ */
