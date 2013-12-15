#ifndef SWIFT_THREAD_H_
#define SWIFT_THREAD_H_

#include "swift-client.h"

/* Types of test data with which to populate a Swift object */
enum test_data_type {
	SIMPLE_TEXT,  /* Simple text, easily identifiable in the Swift object's data */
	ALL_ZEROES,    /* Null bytes */
	PSEUDO_RANDOM /* Pseudo-random bits */
};

/**
 * In/out parameters to a Swift thread.
 */
struct swift_thread_args {
	swift_context_t *swift;         /* Swift library context */
	unsigned int debug;             /* Whether to enable Swift client library debugging */
	pthread_t thread_id;            /* pthread thread ID */
	unsigned int thread_num;        /* Swift thread index */
	const char *proxy;              /* Proxy to use, or NULL for none */
	const char *swift_url;          /* Public endpoint URL of Swift service */
	const char *auth_token;         /* Authentication token from Keystone */
	enum swift_error scerr;         /* Swift client error encountered */
	pthread_cond_t start_condvar;   /* Wait for this condition before starting */
	pthread_mutex_t start_mutex;    /* Protects access to start condvar */
	enum test_data_type data_type;  /* Type of test data with which to fill Swift objects */
	size_t data_size;               /* Length of each Swift object */
	unsigned int num_iterations;    /* Number of sequential identical get and number of put operations */
	unsigned int verify_data;       /* Whether to verify that retrieved data is that which was previously inserted */
	struct timespec start_time;     /* Time of start of Swift thread */
	struct timespec start_put_time; /* Time of start of all put operations */
	struct timespec end_put_time;   /* Time of end of all put operations */
	struct timespec start_get_time; /* Time of start of all get operations */
	struct timespec end_get_time;   /* Time of end of all get operations */
	struct timespec end_time;       /* Time of end of Swift thread */
};

void *swift_thread_func(void *arg);

#endif /* SWIFT_THREAD_H_ */
