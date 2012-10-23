#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <math.h>

#include "sketches.h"
#include "avl.h"
#include "murmur.h"

int fieldsInitialized = 0;
AVL * fields;

int field_t_init(field_t * field, unsigned int width, unsigned int depth) {

    field->width = width;
    field->depth = depth;

    unsigned long ** rows = (unsigned long**)malloc(depth * sizeof(unsigned long*));
    if (NULL == rows) {
        return -1; /* error */
    }

    unsigned int a;
    unsigned int b;
    unsigned long * row;
    for (a = 0; a < depth; a++) {
        row = (unsigned long*)malloc(width * sizeof(unsigned long));
        for (b = 0; b < width; b++) {
            row[b] = 0;
        }
        rows[a] = row;
    }

    field->rows = rows;
    return 1;
}

int field_t_destroy(field_t * field) {
    free(field);
    return 1;
}

int field_t_insert(field_t * field, char * value) {

    uint32_t seed = 0;
    int len = strlen(value);
    uint32_t offset = 0;

    for (seed = 0; seed < field->depth; seed++) {
        MurmurHash3_x86_32(value, len, seed, &offset);
        field->rows[seed][offset % field->width]++;
    }


    return 1;
}

unsigned long field_t_query(field_t * field, char * value) {

    uint32_t seed = 0;
    int len = strlen(value);
    uint32_t offset = 0;
    unsigned long min = 0xFFFFFFFFFFFFFFFF;
    unsigned long tmp = 0;

    for (seed = 0; seed < field->depth; seed++) {
        MurmurHash3_x86_32(value, len, seed, &offset);
        tmp = field->rows[seed][offset % field->width];
        if (tmp < min) {
            min = tmp;
        }
    }

    return min;
}

int dropField(char * name) {
    int ret;

    if (!fieldsInitialized) {
        errno = EDOESNTEXIST;
        return -1;
    }

    field_t * field;
    field = avl_lookup(fields, name);
    if (field != NULL) {
        avl_remove(fields, name);
        field_t_destroy(field);
        return 1;
    }

    errno = EDOESNTEXIST;
    return -1;
}

int newField(char * name, unsigned int width, unsigned int depth) {
    int ret;

    if (!fieldsInitialized) {
        fields = avl_new((AvlCompare) strcmp);
        fieldsInitialized = 1;
    }

    field_t * field;

    field = avl_lookup(fields, name);
    if (field == NULL) {
        ret = field_t_init(field, width, depth);
        if (ret == -1) {
            errno = EINITFAIL;
            return -1; /* OH NOES! */
        }
        avl_insert(fields, strdup(name), field);
    } else {
        errno = ENAMETAKEN;
        return -1;
    }

    return 1;
}

int insertIntoField(char * name, char * value) {
    int ret;

    field_t * field = avl_lookup(fields, name);
    if (field == NULL) {
        errno = EDOESNTEXIST;
        return -1;
    }

    ret = field_t_insert(field, value);
    if (ret == -1) {
        errno = EINSERTIONFAIL;
        return -1;
    }
    return 1;
}

unsigned long queryFromField(char * name, char * value) {
    int ret;

    field_t * field = avl_lookup(fields, name);
    if (field == NULL) {
        errno = EDOESNTEXIST;
        return -1;
    }

    return field_t_query(field, value);
}
