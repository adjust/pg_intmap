#ifndef PG_INTMAP_H
#define PG_INTMAP_H

#include <stdint.h>
#include <string.h>  /* memcpy */


#define VI_MASK (1 << 7)
#define INT64_BITSIZE (sizeof(int64_t) << 3)


typedef struct {
    uint8_t    *buf;
    uint64_t    mask;
    uint8_t     num_bits;
    uint8_t     bits_read;
    uint64_t    reg;
} BitpackIter;

typedef struct {
    bool        first;
    bool        delta_signed;
    int64_t     base;
    BitpackIter bp_iter;
}  DeltaIter;


inline uint64_t zigzag_encode(int64_t value)
{
    return (value << 1) ^ (value >> (INT64_BITSIZE - 1));
}

inline int64_t zigzag_decode(uint64_t value)
{
    /*
     * Operator '>>' on a signed integer replicates the highest bit,
     * so (x << 63 >> 63) would return either bitmask consisting of all 0s
     * or all 1s depending on the sign bit.
     */
    return ((int64_t) value << INT64_BITSIZE - 1 >> INT64_BITSIZE - 1) ^ (value >> 1);
}

inline uint8_t *varint_encode(uint8_t *buf, uint64_t val)
{
    uint64_t res = 0;

    while (val > 0x7f) {
        *buf++ = VI_MASK | 0x7f & (uint8_t) val;
        val >>= 7;
    }
    *buf = 0x7f & (uint8_t) val;

    return ++buf;
}

inline uint8_t *varint_decode(uint8_t *buf, uint64_t *out)
{
    uint64_t res = 0;
    uint8_t i = 0;

    while (*buf & VI_MASK)
        res |= (uint64_t)(*buf++ & ~VI_MASK) << (7 * i++);

    res |= (uint64_t)(*buf & ~VI_MASK) << (7 * i); 
    *out = res;

    return buf + 1;
}

inline uint8_t *bitpack_encode(uint8_t *buf, const uint64_t *vals, uint32_t nvals, uint8_t num_bits)
{
    uint32_t i = 0;
    uint64_t t = 0;
    uint64_t mask = ~((uint64_t)-1 << num_bits);
    uint8_t bits_used = 0;

    while(i < nvals)
    {
        t |= (vals[i] & mask) << bits_used;
        bits_used += num_bits;
        
        if (bits_used >= INT64_BITSIZE)
        {
            uint8_t diff = bits_used - INT64_BITSIZE;

            memcpy(buf, (void *) &t, sizeof(uint64_t));
            buf += sizeof(uint64_t);

            t = (vals[i] & mask) >> (num_bits - diff);
            bits_used = diff;
        }
        i++;
    }

    /* write the remainder */
    if (bits_used > 0) {
        /*
         * Calculate the actual size of the remainder in bytes:
         * (bits_used + sizeof(uint8_t) - 1) / sizeof(uint8_t)
         */
        uint8_t bytes_used = (bits_used + 7) >> 3;

        memcpy(buf, (void *) &t, bytes_used);
        buf += bytes_used;
    }

    return buf;
}

inline uint8_t *bitpack_decode(uint8_t *buf, uint64_t *out, uint32_t nvals, uint8_t num_bits)
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
        else
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

inline void bitpack_iter_init(BitpackIter *it, uint8_t *buf, uint8_t num_bits)
{
    it->buf = buf;
    it->mask = ~((uint64_t)-1 << num_bits);
    it->num_bits = num_bits;
    it->bits_read = 0;
    memcpy(&it->reg, buf, sizeof(uint64_t));
}

inline uint8_t *bitpack_iter_finish(BitpackIter *it)
{
    /*
     * how many bytes are read:
     * (bits + sizeof(uint8_t) - 1) / sizeof(uint8_t)
     */
    uint8_t bytes_read = (it->bits_read + 7) >> 3;

    return it->buf + bytes_read;

}

inline uint64_t bitpack_iter_next(BitpackIter *it)
{
    uint64_t out;

    out = it->reg & it->mask;
    it->bits_read += it->num_bits;

    if (it->bits_read > INT64_BITSIZE) {
        uint8_t diff = it->bits_read - INT64_BITSIZE;
        uint8_t shift = it->num_bits - diff;

        it->buf += sizeof(uint64_t);
        memcpy(&it->reg, it->buf, sizeof(uint64_t));

        out |= (it->reg & (it->mask >> shift)) << shift;
        it->reg >>= diff;
        it->bits_read = diff;
    }
    else
        it->reg >>= it->num_bits;

    return out;
}

/*
 * Note: modifies the original array!
 */
inline uint8_t *delta_encode(uint8_t *buf, int64_t *vals, uint32_t n,
                             uint8_t delta_num_bits, bool delta_signed)
{
    int64_t base;

    Assert(n > 0);
    base = vals[0];
    buf = varint_encode(buf, base < 0 ? zigzag_encode(base) : (uint64_t) base);

    /* calculate deltas */
    for (uint32_t i = 1; i < n; ++i) {
        int64_t new_base = vals[i];
        int64_t delta = vals[i] - base;

        vals[i] = delta_signed ? (int64_t) zigzag_encode(delta) : delta;
        base = new_base;
    }
    buf = bitpack_encode(buf, vals + 1, n - 1, delta_num_bits);

    return buf;
}

inline uint8_t *delta_decode(uint8_t *buf, int64_t *vals, uint32_t n,
                             uint8_t delta_num_bits, bool delta_signed)
{
    buf = varint_decode(buf, &vals[0]);
    buf = bitpack_decode(buf, vals + 1, n - 1, delta_num_bits);
    for (uint32_t i = 1; i < n; ++i) {
        uint64_t delta = delta_signed ? zigzag_decode(vals[i]) : vals[i];

        vals[i] = vals[i - 1] + delta;
    }
}

inline void delta_iter_init(DeltaIter *it, uint8_t *buf, bool base_signed,
                            uint8_t delta_num_bits, bool delta_signed)
{
    buf = varint_decode(buf, &it->base);
    it->base = base_signed ? zigzag_decode(it->base) : it->base;
    it->first = true;
    it->delta_signed = delta_signed;
    bitpack_iter_init(&it->bp_iter, buf, delta_num_bits);
}

inline uint8_t *delta_iter_finish(DeltaIter *it)
{
    return bitpack_iter_finish(&it->bp_iter);
}

inline uint64_t delta_iter_next(DeltaIter *it)
{
    uint64_t res;

    it->base = !it->first ?
        it->base + (
                it->delta_signed ?
                    zigzag_decode(bitpack_iter_next(&it->bp_iter))
                    : (int64_t) bitpack_iter_next(&it->bp_iter))
        : it->base;
    it->first = false;

    return it->base;
}

#endif
