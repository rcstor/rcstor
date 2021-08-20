//
// Created by msn on 17-10-14.
//
#ifndef REED_SOLOMON_H
#define REED_SOLOMON_H

#include <inttypes.h>
#include <stdbool.h>

struct rs_conf_t {
	int n;
	int k;
	int r;

	void *(*allocator)(size_t);

	void (*deallocator)(void *);

	uint8_t **matrix; //matrix of coeficients for encoding

};
typedef struct rs_conf_t rs_conf;

struct rs_decode_context_t {
	uint8_t **inv_matrix;
	int *recover_id;
};
typedef struct rs_decode_context_t rs_decode_context;


void rs_init(rs_conf *conf, int n, int k, void *(allocator)(size_t), void (*deallocator)(void *));
void rs_init_with_default_allocator(rs_conf *conf, int n, int k);
void rs_decode(int len, rs_conf *conf, rs_decode_context *context,uint8_t **data, uint8_t **output);
void rs_decode_context_init(rs_conf *conf, rs_decode_context *context,int *broken_id, int broken_num);
void rs_free_decode_context(rs_conf *conf, rs_decode_context *context);
void rs_free_conf(rs_conf *conf);

#endif //REED_SOLOMON_H
