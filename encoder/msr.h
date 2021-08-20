//
// Created by syd on 16-11-1.
//

#ifndef MSR_H
#define MSR_H

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>

struct msr_conf_t {
    int n;
    int k;
    int r;

    int alpha;
    int beta;
    int groups;

    void *(*allocate)(size_t);

    void (*deallocate)(void *);

    uint8_t *node_companion;
    uint8_t *z_companion;
    uint8_t *theta;

    int nodes_round_up;
};

typedef struct msr_conf_t msr_conf;

struct msr_encode_context_t {
    size_t encoding_buf_size;

    uint8_t *matrix;

    int *survived;
    int survive_cnt;


    int *erased;
    int erase_cnt;

    bool *is_erased;
    int *erase_id;

    int *sigmas;
    int sigma_max;

};

typedef struct msr_encode_context_t msr_encode_context;

struct msr_regenerate_context_t {
    size_t regenerate_buf_size;

    uint8_t *matrix;
    uint8_t *u_matrix;

    int broken;

    int *z_num;
    int *z_pos;
    int *z_comp_pos;


};

typedef struct msr_regenerate_context_t msr_regenerate_context;


void msr_fill_encode_context(msr_encode_context *context, const msr_conf *conf, uint8_t **data);

void msr_fill_regenerate_context(msr_regenerate_context *context, const msr_conf *conf, int broken);

/**@brief Fill the unavailable data.
 * @param len Length of each block of data. len should be at least 512 * alpha.
 * @param n The number of total blocks.
 * @param k The number of systematic blocks.
 * @param data Array of pointers to data buffer. If the pointer is NULL,the corresponding data will be generated.
 * @param output Array of pointers to the output memory.
 * @returns none
 */
void msr_encode(int len, const msr_encode_context *context, const msr_conf *conf, uint8_t *buf, uint8_t **data,
                uint8_t **output);

/**@brief Regenerate the unavailable data.
 * @param len Length of each block of data. len should be the length of collected data.
 * @param n The number of total blocks.
 * @param k The number of systematic blocks.
 * @param data Array of pointers to data buffer. If the pointer is NULL,the corresponding data will be generated.
 * @param output The address to the regenerated data.
 * @returns none
 */
void msr_regenerate(int len, const msr_regenerate_context *context, const msr_conf *conf, uint8_t *buf, uint8_t **data,
                    uint8_t *output);

void msr_get_regenerate_offset(int len, const msr_regenerate_context *context, const msr_conf *conf, int *offsets);

int msr_init_with_default_allocator(msr_conf *conf, int n, int k);
int msr_init(msr_conf *conf, int n, int k, void *(*allocate)(size_t), void (*deallocate)(void *));

void msr_free_conf(msr_conf *conf);
void msr_free_encode_context(const msr_conf *conf,msr_encode_context *context);
void msr_free_regenerate_context(const msr_conf *conf,msr_regenerate_context *context);

#endif //MSR_H