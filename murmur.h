#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_

#include <stdint.h>


void MurmurHash3_x86_32(const void * key, int len, uint32_t seed, uint32_t * out);


#endif // _MURMURHASH3_H_

