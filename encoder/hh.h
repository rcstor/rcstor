//
// Created by gty on 17-8-25.
//

#ifndef LIBMSR_HH_H
#define LIBMSR_HH_H

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
struct hh_conf_t {
    int n;
    int k;
    int r;
    uint8_t **matrix;
    void *(*allocate)(size_t);
    void (*deallocate)(void *);
};
typedef struct hh_conf_t hh_conf;

struct hh_encode_context_t {

};
typedef struct hh_encode_context_t hh_encode_context;


struct hh_regenerate_context_t {
    int broken;
    int* size;
    int* offset;
};

typedef struct hh_regenerate_context_t hh_regenerate_context;

void hh_init_with_default_allocator(hh_conf *conf, int n, int k);
void hh_init(hh_conf *conf, int n, int k, void *(*allocator)(size_t), void (*deallocator)(void *));
void hh_encode(int len, hh_conf *conf, uint8_t **data, uint8_t **output);
void hh_get_regenerate_offset(int len, hh_regenerate_context *context, hh_conf *conf, int broken);
void hh_regenerate(int len, hh_regenerate_context *context, hh_conf *conf, uint8_t **data, uint8_t *output);
void hh_free_regenerate_context(hh_conf *conf, hh_regenerate_context *context);
void hh_free_conf(hh_conf *conf);
#endif //LIBMSR_HH_H

