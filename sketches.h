#ifndef SKETCHES_H
#define SKETCHES_H

typedef struct _field {
    unsigned int width;
    unsigned int depth;
    unsigned long ** rows;
} field_t;

int field_t_init(field_t *, unsigned int, unsigned int);
int field_t_destroy(field_t *);

int newField(char *, unsigned int, unsigned int);
int dropField(char *);

int insertIntoField(char *, char *);
unsigned long queryFromField(char *, char *);

// error nums for creating new fields.
#define ENAMETAKEN 100
#define EINITFAIL 101
#define EDOESNTEXIST 102
#define EINSERTIONFAIL 103

#endif
