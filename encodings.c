#include "encodings.h"

#include <string.h>  /* memcpy */


#define LEB_MASK (1 << 7)

#define INT64_BITSIZE (sizeof(int64_t) << 3)

uint8_t *varint_encode(uint8_t *buf, uint64_t val)
{
    int      bits_left = INT64_BITSIZE - __builtin_clzl(val);
    uint64_t res = 0;

    while (bits_left > 7) {
        *buf = LEB_MASK | 0x7f & (uint8_t) val;
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

uint8_t *bitpack_encode(uint8_t *buf, const uint64_t *vals, uint32_t nvals, uint8_t num_bits)
{
    uint32_t i = 0;
    uint64_t t = 0;
    uint64_t mask = ~((uint64_t)-1 << num_bits);
    uint8_t bits_used = 0;

    while(i < nvals)
    {
        t |= (vals[i] & mask) << bits_used;
        bits_used += num_bits;
        
        if (bits_used > INT64_BITSIZE)
        {
            uint8_t diff = bits_used - INT64_BITSIZE;

            memcpy(buf, (void *) &t, sizeof(uint64_t));
            buf += sizeof(uint64_t);

            t = (vals[i] & mask) >> (num_bits - diff);
            bits_used = diff;
        }
        i++;
    }

    if (bits_used > 0)
        memcpy(buf, (void *) &t, sizeof(uint64_t));

    /* 
     * how many bytes used:
     * (bits + sizeof(uint8_t) - 1) / sizeof(uint8_t)
     */
    uint8_t bytes_used = (bits_used + 7) >> 3;
    return buf + bytes_used;
}

const uint8_t *bitpack_decode(const uint8_t *buf, uint64_t *out, uint32_t nvals, uint8_t num_bits)
{
    uint32_t i = 0;
    uint64_t mask = ~((uint64_t)-1 << num_bits);
    uint8_t bits_read = 0;
    uint64_t t;

    memcpy(&t, buf, sizeof(uint64_t));
    while (i < nvals) {
        *out = t & mask;
        bits_read += num_bits;

        if (bits_read > INT64_BITSIZE) {
            uint8_t diff = bits_read - INT64_BITSIZE;
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

    /* 
     * how many bytes are read:
     * (bits + sizeof(uint8_t) - 1) / sizeof(uint8_t)
     */
    uint8_t bytes_read = (bits_read + 7) >> 3;
    return buf + bytes_read;
}

uint64_t zigzag_encode(int64_t value)
{
    return (value << 1) ^ (value >> (INT64_BITSIZE - 1));
}

int64_t zigzag_decode(uint64_t value)
{
    /*
     * Operator '>>' on a signed integer replicates the highest bit,
     * so (x << 63 >> 63) would return either bitmask consisting of all 0s
     * or all 1s depending on the sign bit.
     */
    return ((int64_t) value << INT64_BITSIZE - 1 >> INT64_BITSIZE - 1) ^ (value >> 1);
}
