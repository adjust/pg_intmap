#include "encodings.h"

#include <string.h>  /* memcpy */


#define LEB_MASK (1 << 7)

uint8_t *varint_encode(uint8_t *buf, uint64_t val)
{
    int      bits_left = 64 - __builtin_clzl(val);
    uint64_t res = 0;

    while (bits_left > 7) {
        *buf = (1 << 7) | 0x7f & (uint8_t) val;
        val >>= 7;
        buf++;
        bits_left -= 7;
    }
    *buf = 0x7f & (uint8_t) val;

    return ++buf;
}

uint8_t *varint_decode(uint8_t *buf, uint64_t *out)
{
    uint64_t res = 0;
    uint8_t i = 0;

    while (*buf & LEB_MASK) {
        res |= (uint64_t)(*buf & ~LEB_MASK) << (7 * i);
        buf++;
        i++;
    }
    res |= (uint64_t)(*buf & ~LEB_MASK) << (7 * i); 
    *out = res;

    return buf + 1;
}

void bitpack_encode(uint8_t *buf, const uint64_t *vals, uint32_t nvals, uint8_t num_bits)
{
    uint32_t i = 0;
    uint64_t t = 0;
    uint64_t mask = ~(-1 << num_bits);
    uint8_t bits_used = 0;

    while(i < nvals)
    {
        t |= (vals[i] & mask) << bits_used;
        bits_used += num_bits;
        
        if (bits_used > sizeof(uint64_t) * 8)
        {
            uint8_t diff = bits_used - sizeof(uint64_t) * 8;

            memcpy(buf, (void *) &t, sizeof(uint64_t));
            buf += sizeof(uint64_t);

            t = (vals[i] & mask) >> (num_bits - diff);
            bits_used = diff;
        }
        i++;
    }

    if (bits_used > 0)
        memcpy(buf, (void *) &t, sizeof(uint64_t));
}

void bitpack_decode(const uint8_t *buf, uint64_t *out, uint32_t nvals, uint8_t num_bits)
{
    uint32_t i = 0;
    uint64_t mask = ~(-1 << num_bits);
    uint8_t bits_read = 0;
    uint64_t t;

    memcpy(&t, buf, sizeof(uint64_t));
    while (i < nvals) {
        *out = t & mask;
        bits_read += num_bits;

        if (bits_read > sizeof(uint64_t) * 8) {
            uint8_t diff = bits_read - sizeof(uint64_t) * 8;
            uint8_t shift = num_bits - diff; 

            buf += sizeof(uint64_t);
            memcpy(&t, buf, sizeof(uint64_t));

            *out |= (t & (mask >> shift)) << shift;
            t >>= shift;
            bits_read = diff;
        }
        t >>= num_bits;
        out++;
        i++;
    }
}
