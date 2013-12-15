#ifndef KEYSTONE_THREAD_H_
#define KEYSTONE_THREAD_H_

#include "keystone-client.h"

/* In/out parameters to a Keystone thread */
struct keystone_thread_args {
	keystone_context_t keystone;  /* Keystone client library context */
	pthread_t thread_id;          /* pthread thread ID */
	unsigned int debug;           /* Whether to enable Keystone client library debugging */
	const char *proxy;            /* Proxy to use, or NULL for none */
	const char *url;              /* Keystone service's public endpoint URL */
	const char *tenant;           /* Tenant name for authentication */
	const char *username;         /* Username for authentication */
	const char *password;         /* Password for authentication */
	char *auth_token;             /* Out: Authentication token from Keystone service */
	char *swift_url;              /* Out: Swift service's public endpoint URL */
	enum keystone_error kserr;    /* Keystone client library error encountered */
};

void *keystone_thread_func(void *arg);

#endif /* KEYSTONE_THREAD_H_ */
