// C interface to the sia_storage Rust crate.
//
// Conventions:
//   - Fallible functions return an int32_t status code (SIA_OK on success)
//     and set *err to a heap-allocated message on failure. Free it with
//     sia_string_free.
//   - Handles are opaque pointers owned by the caller and released with the
//     matching *_free function.
//   - Blocking functions accept an optional sia_cancel_t. Cancelling the
//     token unblocks the call with SIA_ERR_CANCELLED.
//   - Timestamps are Unix microseconds (UTC).
#ifndef SIA_STORAGE_H
#define SIA_STORAGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
	SIA_OK = 0,
	SIA_ERR = 1,
	SIA_ERR_UNAUTHORIZED = 2,
	SIA_ERR_USER_REJECTED = 3,
	SIA_ERR_REQUEST_EXPIRED = 4,
	SIA_ERR_CANCELLED = 5,
	SIA_ERR_INVALID_STATE = 6,
};

typedef struct sia_builder sia_builder_t;
typedef struct sia_sdk sia_sdk_t;
typedef struct sia_object sia_object_t;
typedef struct sia_upload sia_upload_t;
typedef struct sia_download sia_download_t;
typedef struct sia_packed_upload sia_packed_upload_t;
typedef struct sia_events sia_events_t;
typedef struct sia_cancel sia_cancel_t;
typedef struct sia_mock sia_mock_t;

typedef struct {
	uint8_t host_key[32];
	uint64_t shard_size;
	uint64_t shard_index;
	uint64_t slab_index;
	uint64_t elapsed_us;
} sia_shard_progress_t;

// Invoked from arbitrary Rust runtime threads. Implementations must be
// thread-safe, must not call back into this library, and must not retain or
// mutate the pointed-to data past the call.
typedef void (*sia_progress_cb_t)(uintptr_t userdata, sia_shard_progress_t* progress);

// level: 1=error 2=warn 3=info 4=debug 5=trace
typedef void (*sia_log_cb_t)(uintptr_t userdata, int32_t level, char* target, char* message);

typedef struct {
	uint8_t data_shards;   // ignored unless set_redundancy
	uint8_t parity_shards; // ignored unless set_redundancy
	bool set_redundancy;
	uint64_t max_buffered_slabs; // 0 = default
	sia_progress_cb_t on_shard;  // may be NULL
	uintptr_t userdata;
} sia_upload_options_t;

typedef struct {
	uint64_t offset;
	bool has_length;
	uint64_t length;              // ignored unless has_length
	uint64_t max_buffered_chunks; // 0 = default
	sia_progress_cb_t on_shard;   // may be NULL
	uintptr_t userdata;
} sia_download_options_t;

void sia_string_free(char* s);

// Installs a process-wide logger bridging the Rust `log` crate.
// May only be called once; subsequent calls are ignored.
void sia_set_logger(sia_log_cb_t cb, uintptr_t userdata, int32_t max_level);

// Returns a new BIP-39 12-word recovery phrase. Free with sia_string_free.
char* sia_generate_recovery_phrase(void);

sia_cancel_t* sia_cancel_new(void);
void sia_cancel_cancel(sia_cancel_t* c);
void sia_cancel_free(sia_cancel_t* c);

// app_meta_json: {"appID":"<hex>","name":...,"description":...,"serviceURL":...,
// "logoURL":...|null,"callbackURL":...|null}
int32_t sia_builder_new(const char* indexer_url, const char* app_meta_json, sia_builder_t** out, char** err);
void sia_builder_free(sia_builder_t* b);
// Returns SIA_ERR_UNAUTHORIZED when the app key is not authorized.
int32_t sia_builder_connect(sia_builder_t* b, const uint8_t app_key[32], sia_cancel_t* cancel, sia_sdk_t** out, char** err);
int32_t sia_builder_request_connection(sia_builder_t* b, sia_cancel_t* cancel, char** response_url, char** err);
int32_t sia_builder_wait_for_approval(sia_builder_t* b, sia_cancel_t* cancel, char** err);
int32_t sia_builder_register(sia_builder_t* b, const char* mnemonic, sia_cancel_t* cancel, sia_sdk_t** out, char** err);

void sia_sdk_free(sia_sdk_t* sdk);
void sia_sdk_app_key(const sia_sdk_t* sdk, uint8_t out[32]);
// *out_json receives the account encoded as JSON. Free with sia_string_free.
int32_t sia_sdk_account(const sia_sdk_t* sdk, sia_cancel_t* cancel, char** out_json, char** err);
int32_t sia_sdk_object(const sia_sdk_t* sdk, const uint8_t id[32], sia_cancel_t* cancel, sia_object_t** out, char** err);
int32_t sia_sdk_object_events(const sia_sdk_t* sdk, bool has_cursor, int64_t after_unix_us, const uint8_t after_id[32], uint64_t limit, sia_cancel_t* cancel, sia_events_t** out, char** err);
int32_t sia_sdk_pin_object(const sia_sdk_t* sdk, const sia_object_t* obj, sia_cancel_t* cancel, char** err);
int32_t sia_sdk_update_object_metadata(const sia_sdk_t* sdk, const sia_object_t* obj, sia_cancel_t* cancel, char** err);
int32_t sia_sdk_delete_object(const sia_sdk_t* sdk, const uint8_t id[32], sia_cancel_t* cancel, char** err);
int32_t sia_sdk_prune_slabs(const sia_sdk_t* sdk, sia_cancel_t* cancel, char** err);
int32_t sia_sdk_share_object(const sia_sdk_t* sdk, const sia_object_t* obj, int64_t valid_until_unix_us, char** out_url, char** err);
int32_t sia_sdk_shared_object(const sia_sdk_t* sdk, const char* share_url, sia_cancel_t* cancel, sia_object_t** out, char** err);

sia_object_t* sia_object_new(void);
void sia_object_free(sia_object_t* o);
void sia_object_id(const sia_object_t* o, uint8_t out[32]);
uint64_t sia_object_size(const sia_object_t* o);
uint64_t sia_object_encoded_size(const sia_object_t* o);
int64_t sia_object_created_at(const sia_object_t* o);
int64_t sia_object_updated_at(const sia_object_t* o);
// Returns the metadata length. If buf is non-NULL and cap is large enough,
// copies the metadata into buf.
size_t sia_object_metadata(const sia_object_t* o, uint8_t* buf, size_t cap);
void sia_object_set_metadata(sia_object_t* o, const uint8_t* data, size_t len);

size_t sia_events_len(const sia_events_t* evs);
// Transfers ownership of the event's object (NULL for deletions) to the
// caller. Call at most once per index.
void sia_events_at(sia_events_t* evs, size_t i, uint8_t id_out[32], bool* deleted, int64_t* updated_at_unix_us, sia_object_t** obj);
void sia_events_free(sia_events_t* evs);

// The upload streams data pushed via sia_upload_write. Call sia_upload_finish
// to signal EOF and wait for completion; it returns the updated object.
// sia_upload_free aborts the upload if it is still running.
int32_t sia_upload_start(const sia_sdk_t* sdk, const sia_object_t* obj, const sia_upload_options_t* opts, sia_upload_t** out, char** err);
int32_t sia_upload_write(sia_upload_t* up, const uint8_t* data, size_t len, sia_cancel_t* cancel, char** err);
int32_t sia_upload_finish(sia_upload_t* up, sia_cancel_t* cancel, sia_object_t** out, char** err);
void sia_upload_free(sia_upload_t* up);

// sia_download_read blocks until at least one byte is available and then
// opportunistically fills as much of buf as is ready without blocking again.
// *n == 0 signals EOF. sia_download_free cancels any in-flight recovery; it
// must not race a blocked sia_download_read — cancel first.
int32_t sia_download_start(const sia_sdk_t* sdk, const sia_object_t* obj, const sia_download_options_t* opts, sia_download_t** out, char** err);
int32_t sia_download_read(sia_download_t* dl, uint8_t* buf, size_t cap, sia_cancel_t* cancel, size_t* n, char** err);
void sia_download_free(sia_download_t* dl);

// Objects are added one at a time: add_begin, then add_write until the
// object's data is exhausted, then add_finish (which reports the number of
// bytes packed). finalize returns the packed objects.
int32_t sia_packed_upload_start(const sia_sdk_t* sdk, const sia_upload_options_t* opts, sia_packed_upload_t** out, char** err);
uint64_t sia_packed_upload_remaining(const sia_packed_upload_t* up);
uint64_t sia_packed_upload_length(const sia_packed_upload_t* up);
uint64_t sia_packed_upload_optimal_data_size(const sia_packed_upload_t* up);
int32_t sia_packed_upload_add_begin(sia_packed_upload_t* up, char** err);
int32_t sia_packed_upload_add_write(sia_packed_upload_t* up, const uint8_t* data, size_t len, sia_cancel_t* cancel, char** err);
int32_t sia_packed_upload_add_finish(sia_packed_upload_t* up, sia_cancel_t* cancel, uint64_t* written, char** err);
// *out_objs receives a heap array of owned object handles. Free the array
// (not the objects) with sia_object_array_free.
int32_t sia_packed_upload_finalize(sia_packed_upload_t* up, sia_cancel_t* cancel, sia_object_t*** out_objs, size_t* out_len, char** err);
void sia_object_array_free(sia_object_t** objs, size_t len);
void sia_packed_upload_free(sia_packed_upload_t* up);

// In-memory mock backend for tests: real erasure coding, encryption, and
// upload/download pipelines against fake in-process hosts.
sia_mock_t* sia_mock_new(size_t num_hosts, const uint8_t app_key[32]);
void sia_mock_free(sia_mock_t* m);
int32_t sia_mock_upload_start(const sia_mock_t* m, const sia_object_t* obj, const sia_upload_options_t* opts, sia_upload_t** out, char** err);
int32_t sia_mock_download_start(const sia_mock_t* m, const sia_object_t* obj, const sia_download_options_t* opts, sia_download_t** out, char** err);
int32_t sia_mock_packed_upload_start(const sia_mock_t* m, const sia_upload_options_t* opts, sia_packed_upload_t** out, char** err);

#ifdef __cplusplus
}
#endif

#endif // SIA_STORAGE_H
