#include <stdint.h>
#include "define.h"

inline void bswap8(char *input)
{
    if (IS_BIG_ENDIAN)
        return;

    char tmp;

    tmp = input[0];
    input[0] = input[7];
    input[7] = tmp;

    tmp = input[1];
    input[1] = input[6];
    input[6] = tmp;

    tmp = input[2];
    input[2] = input[5];
    input[5] = tmp;

    tmp = input[3];
    input[3] = input[4];
    input[4] = tmp;
}

inline void bswap4(char *input)
{
    if (IS_BIG_ENDIAN)
        return;

    uint32_t *the_num= (uint32_t*)input;
    *the_num= ntohl(*the_num);
}

inline void bswap2(char *input)
{
    if (IS_BIG_ENDIAN)
        return;

    uint16_t *the_num= (uint16_t*)input;
    *the_num= ntohs(*the_num);
}

