#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "utils/array.h"

#include "encodings.h"


PG_MODULE_MAGIC;

#define INTMAP_VERSION      0

#define PLAIN_ENCODING      0
#define VARINT_ENCODING     1
#define BITPACK_ENCODING    2
#define DELTA_ENCODING      3
#define ZIGZAG_ENCODING     8


typedef struct
{
    uint64_t    nitems;
    uint64_t    valoff;  /* values offset */
    uint8_t     key_enc;
    uint8_t     val_enc;
    uint8_t     version;
} IntMapHeader;

typedef struct
{
    uint32_t varint_size;   /* bytes required to store all values in varint */
    uint32_t bitpack_size;  /* bytes required to store all values in bitpack */
    uint32_t delta_size;
    uint8_t  num_bits;      /* sufficient number of bits per value to encode
                               using bit packing */
    uint8_t  delta_num_bits;
    uint32_t best_size;
    uint8_t  best_encoding;
    bool     is_signed;     /* contains signed values */
    bool     is_delta_signed; /* deltas contain signed values */
} ArrayStats;

/*
 * encodings.h declarations
 *
 * These may not be needed when optimization is enabled and functions are
 * properly inlined. But just in case this isn't the case (and for debugging
 * purposes)
 */
uint8_t *varint_encode(uint8_t *buf, uint64_t val);
uint8_t *varint_decode(uint8_t *buf, uint64_t *out);
uint8_t *bitpack_encode(uint8_t *buf, const uint64_t *vals, uint32_t nvals, uint8_t num_bits);
uint8_t *bitpack_decode(uint8_t *buf, uint64_t *out, uint32_t nvals, uint8_t num_bits);
uint8_t *delta_encode(uint8_t *buf, int64_t *vals, uint32_t n,
                      uint8_t delta_num_bits, bool delta_signed);
uint8_t *delta_decode(uint8_t *buf, int64_t *vals, uint32_t n,
                      uint8_t delta_num_bits, bool delta_signed);
uint64_t zigzag_encode(int64_t value);
int64_t zigzag_decode(uint64_t value);
void bitpack_iter_init(BitpackIter *it, uint8_t *buf, uint8_t num_bits);
uint64_t bitpack_iter_next(BitpackIter *it);
uint8_t *bitpack_iter_finish(BitpackIter *it);
void delta_iter_init(DeltaIter *it, uint8_t *buf, bool base_signed,
                     uint8_t delta_num_bits, bool delta_signed);
uint64_t delta_iter_next(DeltaIter *it);
uint8_t *delta_iter_finish(DeltaIter *it);

/*
 * parse.c declarations
 */
void parse_intmap(const char *c, int64_t **keys, int64_t **values, int *n);
void parse_intarr(const char *c, int64_t **values, int *n);
void intmap_qsort(int64_t *keys, int64_t *values, int32_t n);

static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n);
static Datum create_intarr_internal(uint64_t *values, uint32_t n);

#define MEANINGFUL_BITS(x) ((sizeof(int64_t) << 3) - __builtin_clzl(x))
#define VARINT_SIZE(x) ((MEANINGFUL_BITS(x) + 7 - 1) / 7)

/* ceil(number of bits / 8) */
#define BITPACK_SIZE(n, num_bits) (((n) * (num_bits) + 7) >> 3)

static void collect_stats(ArrayStats *stats, int64_t *vals, uint32_t n)
{
    uint64_t varint_size = 0;
    uint64_t mask = 0;
    uint64_t delta_mask = 0;
    int64_t  delta_base;

    if (n == 0) {
        stats->best_size = 0;
        stats->best_encoding = PLAIN_ENCODING;
        return;
    }

    /* delta encoding base */
    delta_base = vals[0];

    /* check for negative numbers */
    for (uint32_t i = 0; i < n; ++i) {
        mask |= vals[i];
        delta_mask |= vals[i] - delta_base;
        delta_base = vals[i];
    }
    stats->is_signed = mask & ((uint64_t) 1 << 63);
    stats->is_delta_signed = delta_mask & ((uint64_t) 1 << 63);

    mask = delta_mask = 0;
    delta_base = vals[0];
    for (uint32_t i = 0; i < n; ++i) {
        int64_t  delta;
        uint64_t zz_val;    /* zigzaged value */
        uint64_t zz_delta;  /* zigzaged delta */

        delta = vals[i] - delta_base;
        zz_delta = stats->is_delta_signed ? zigzag_encode(delta) : (uint64_t) delta;
        delta_base = vals[i];

        /* encode with zigzag if needed */
        zz_val = stats->is_signed ? zigzag_encode(vals[i]) : (uint64_t) vals[i];

        /* count bytes needed for varint encoding */
        varint_size += VARINT_SIZE(zz_val);

        /*
         * Summarize bitwise values to find the sufficient number of bits to
         * represent them all.
         */
        mask |= zz_val;
        delta_mask |= zz_delta;
    }

    stats->num_bits = MEANINGFUL_BITS(mask);
    stats->delta_num_bits = MEANINGFUL_BITS(delta_mask);

    /* +1 byte for bits length encoding */
    stats->bitpack_size = BITPACK_SIZE(n, stats->num_bits) + 1;

    uint32_t base_size = VARINT_SIZE(vals[0] < 0 ? zigzag_encode(vals[0]) : (uint64_t) vals[0]);
    stats->delta_size = BITPACK_SIZE(n - 1, stats->delta_num_bits) + 1 + base_size;

    stats->varint_size = varint_size;

    /* find the best encoding */
    struct {
        uint32_t size;
        uint8_t  enc;
    } results[] = {
        {stats->bitpack_size, BITPACK_ENCODING},
        {stats->varint_size, VARINT_ENCODING},
        {stats->delta_size, DELTA_ENCODING}
    };

    stats->best_size = results[0].size;
    stats->best_encoding = results[0].enc;
    for (int i = 1; i < sizeof(results) / sizeof(results[0]); ++i) {
        if (results[i].size < stats->best_size) {
            stats->best_size = results[i].size;
            stats->best_encoding = results[i].enc;
        }
    }

    /* Add zigzag encoding if needed */
    switch (stats->best_encoding) {
        case BITPACK_ENCODING:
        case VARINT_ENCODING:
            stats->best_encoding |= stats->is_signed ? ZIGZAG_ENCODING : 0;
            break;
        case DELTA_ENCODING:
            /* only applies to the base */
            stats->best_encoding |= vals[0] < 0 ? ZIGZAG_ENCODING : 0;
            break;
    }
}

static inline uint8_t *write_num_bits(uint8_t *buf, uint8_t num_bits)
{
    *buf = num_bits;
    return ++buf;
}

static inline uint8_t *read_num_bits(uint8_t *buf, uint8_t *num_bits)
{
    *num_bits = *buf;
    return ++buf;
}

/*
 * intmap_read_header
 *      Decode intmap header.
 *
 * intmap header structure:
 * - version (3 bits)
 * - number of items encoded with modified varint (4 bits of the first byte +
 *   1 bit marker potentially followed by a few more bytes);
 * - encodings (8 bits):
 *    > 1 bit:   zigzag encoding for keys (true/false);
 *    > 3 bits:  keys encoding (one of *_ENCODING values);
 *    > 1 bit:   ziazag encoding for values (true/false);
 *    > 3 bits:  values encoding;
 * - values offset encoded using varint
 */
static inline uint8_t *intmap_read_header(uint8_t *buf, IntMapHeader *h)
{
    h->version = *buf >> 5;

    /* read the number of items */
    h->nitems = *buf & 0x0f;
    if (*buf++ & 0x10) {
        uint64_t head;

        buf = varint_decode(buf, &head);
        h->nitems = head << 4 | h->nitems;
    }

    if (h->nitems > 0) {
        /* read encodings */
        h->key_enc = *buf >> 4;
        h->val_enc = *buf++ & 0x0f;

        /* read values offset */
        buf = varint_decode(buf, &h->valoff);
    } else {
        h->key_enc = h->val_enc = PLAIN_ENCODING;
    }

    return buf;
}

/*
 * intmap_write_header
 *      Encode intmap header.
 */
static inline uint8_t *intmap_write_header(uint8_t *buf, IntMapHeader *h)
{
    uint64_t n = h->nitems;

    /* write 3 bits version num */
    *buf = INTMAP_VERSION << 5;

    /*
     * Write the first 4 bits of varint encoded nitems. If the number of items
     * doesn't fit run regular varint on what's left.
     */
    if (n > 0x0f) {
        *buf++ |= 0x10 | 0x0f & (uint8_t) n;
        n >>= 4;

        buf = varint_encode(buf, n);
    } else
        *buf++ |= n;

    if (n > 0) {
        /* write encodings */
        *buf++ = h->key_enc << 4 | (h->val_enc & 0xF);

        /* write values offset */
        buf = varint_encode(buf, h->valoff);
    }

    return buf;
}

/*
 * values are expected to be already zigzaged if needed.
 */
static inline uint8_t *encode_array(uint8_t *buf, ArrayStats *stats,
                                    int64_t *vals, uint32_t n)
{
    Assert(n > 0);

    switch (stats->best_encoding & 0x07) {
        case VARINT_ENCODING:
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t v;

                v = stats->is_signed ? zigzag_encode(vals[i]) : (uint64_t) vals[i];
                buf = varint_encode(buf, v);
            }
            break;
        case BITPACK_ENCODING:
            if (stats->is_signed)
                for (uint32_t i = 0; i < n; ++i)
                    vals[i] = zigzag_encode(vals[i]);
            buf = write_num_bits(buf, stats->num_bits);
            buf = bitpack_encode(buf, vals, n, stats->num_bits);
            break;
        case DELTA_ENCODING:
            /*
             * Write num_bits for deltas and set the most significat bit if
             * deltas are signed
             */
            *buf++ = stats->delta_num_bits | (stats->is_delta_signed ? 0x80 : 0); 
            buf = delta_encode(buf, vals, n, stats->delta_num_bits, stats->is_delta_signed);
            break;
        default:
            elog(ERROR, "unexpected encoding");
    }

    return buf;
}

static inline uint8_t *decode_array(uint8_t *buf, uint8_t encoding,
                                    int64_t *vals, uint32_t n)
{
    switch (encoding & 0x07)
    {
        case VARINT_ENCODING:
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t    v;

                buf = varint_decode(buf, &v);
                vals[i] = encoding & ZIGZAG_ENCODING ? zigzag_decode(v) : (int64_t) v; 
            }
            break;
        case BITPACK_ENCODING:
            {
                uint8_t num_bits;

                buf = read_num_bits(buf, &num_bits);
                buf = bitpack_decode(buf, vals, n, num_bits);

                if (encoding & ZIGZAG_ENCODING)
                    for (uint32_t i = 0; i < n; ++i)
                        vals[i] = zigzag_decode(vals[i]);
                break;
            }
        case DELTA_ENCODING:
            {
                bool    delta_signed;
                uint8_t delta_num_bits;

                delta_signed = *buf & 0x80;
                delta_num_bits = *buf++ & 0x7f;
                buf = delta_decode(buf, vals, n, delta_num_bits, delta_signed);
                break;
            }
        default:
            elog(ERROR, "unsupported encoding");
    }

    return buf;
}

typedef struct {
    uint8_t             encoding;
    bool                is_signed;

    union {
        /* varint */
        struct {
            uint8_t    *buf;
        } varint;

        /* bit packing */
        BitpackIter     bitpack;

        /* delta encoding */
        DeltaIter       delta;
    } u;
} DecoderIter;

static inline void decoder_iter_init(DecoderIter *it, uint8_t encoding, uint8_t *buf)
{
    it->encoding = encoding & 0x7;
    it->is_signed = encoding & 0x8;

    switch (it->encoding)
    {
        case VARINT_ENCODING:
            it->u.varint.buf = buf;
            break;
        case BITPACK_ENCODING:
            {
                uint8_t     num_bits;

                buf = read_num_bits(buf, &num_bits);
                bitpack_iter_init(&it->u.bitpack, buf, num_bits);
                break;
            }
        case DELTA_ENCODING:
            {
                bool    delta_signed;
                uint8_t delta_num_bits;

                delta_signed = *buf & 0x80;
                delta_num_bits = *buf++ & 0x7f;
                delta_iter_init(&it->u.delta, buf, it->is_signed,
                                delta_num_bits, delta_signed);
                break;
            }
        default:
            elog(ERROR, "unsupported encoding");
    }
}

static inline int64_t decoder_iter_next(DecoderIter *it)
{
    int64_t res;

    switch (it->encoding)
    {
        case VARINT_ENCODING:
            it->u.varint.buf = varint_decode(it->u.varint.buf, &res);
            res = it->is_signed ? zigzag_decode(res) : res;
            break;
        case BITPACK_ENCODING:
            res = bitpack_iter_next(&it->u.bitpack);
            res = it->is_signed ? zigzag_decode(res) : res;
            break;
        case DELTA_ENCODING:
            res = delta_iter_next(&it->u.delta);
            break;
        default:
            elog(ERROR, "unsupported encoding");
    }


    return res;
}

static inline uint8_t *decoder_iter_finish(DecoderIter *it)
{
    switch (it->encoding)
    {
        case VARINT_ENCODING:
            return it->u.varint.buf;
        case BITPACK_ENCODING:
            return bitpack_iter_finish(&it->u.bitpack);
        case DELTA_ENCODING:
            return delta_iter_finish(&it->u.delta);
        default:
            elog(ERROR, "unsupported encoding");
    }
}

PG_FUNCTION_INFO_V1(intmap_in);
Datum intmap_in(PG_FUNCTION_ARGS)
{
    char    *in = PG_GETARG_CSTRING(0);
    int64_t *keys;
    int64_t *values;
    int      n;

    parse_intmap(in, &keys, &values, &n);

    return create_intmap_internal(keys, values, n);
}

PG_FUNCTION_INFO_V1(intmap_out);
Datum intmap_out(PG_FUNCTION_ARGS)
{
    Datum        in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint8_t     *data = VARDATA(in);
    IntMapHeader h;
    DecoderIter  k_it, v_it;
    StringInfoData str;

    data = intmap_read_header(data, &h);

    if (h.nitems == 0)
        PG_RETURN_CSTRING("");

    /* iterate through keys/values */
    decoder_iter_init(&k_it, h.key_enc, data);
    decoder_iter_init(&v_it, h.val_enc, data + h.valoff);
    initStringInfo(&str);
    for (uint32_t i = 0; i < h.nitems; ++i) {
        appendStringInfo(&str, i == 0 ? "%ld=>%ld" : ", %ld=>%ld",
                         decoder_iter_next(&k_it),
                         decoder_iter_next(&v_it));
    }

    PG_RETURN_CSTRING(str.data);
}

static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n)
{
    uint8_t    *out;
    uint8_t    *data;
    ArrayStats key_stats, val_stats;
    uint8_t    *keys_start;
    IntMapHeader h;
    Size        sz;

    collect_stats(&key_stats, keys, n);
    collect_stats(&val_stats, values, n);

    /*
     * Estimate size:
     * - 4 byte bytea header;
     * - 3 bits version + 32 bits varint encoded length take up to 5 bytes;
     * - 1 byte for encodings info;
     * - estimated size of encoded keys and values.
     */
    sz = VARHDRSZ + 5 + 1 + key_stats.best_size + val_stats.best_size;
    out = palloc0(sz);
    data = VARDATA(out);

    /* Write header */
    h.version = INTMAP_VERSION;
    h.nitems  = n;
    h.key_enc = key_stats.best_encoding;
    h.val_enc = val_stats.best_encoding;
    h.valoff = key_stats.best_size;
    keys_start = data = intmap_write_header(data, &h);

    /* Encode keys and values */
    if (n > 0) {
        data = encode_array(data, &key_stats, keys, n);
        Assert(data == keys_start + key_stats.best_size);
        data = encode_array(data, &val_stats, values, n);
        Assert(out + sz >= data);
    }

    SET_VARSIZE(out, data - out);
    return PointerGetDatum(out);
}


PG_FUNCTION_INFO_V1(create_intmap);
Datum create_intmap(PG_FUNCTION_ARGS)
{
    ArrayType  *keys_arr = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType  *values_arr = PG_GETARG_ARRAYTYPE_P(1);
    uint64_t   *keys, *values;
    uint32_t    nkeys, nvalues;
    bool       *null_keys, *null_values;

    deconstruct_array(keys_arr, INT8OID, sizeof(int64_t), true, 'd',
                      &keys, &null_keys, &nkeys);

    deconstruct_array(values_arr, INT8OID, sizeof(int64_t), true, 'd',
                      &values, &null_values, &nvalues);

    if (nkeys != nvalues)
        elog(ERROR, "the keys array size does not match the values array size");

    for (uint32_t i = 0; i < nkeys; ++i)
        if (null_keys[i] | null_values[i])
            elog(ERROR, "input arrays must not contain NULLs");

    intmap_qsort(keys, values, nkeys);

    return create_intmap_internal(keys, values, nkeys);
}


PG_FUNCTION_INFO_V1(intmap_get_val);
Datum intmap_get_val(PG_FUNCTION_ARGS)
{
    Datum        in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint64_t     key = DatumGetUInt64(PG_GETARG_DATUM(1));
    uint8_t     *data = VARDATA(in);
    IntMapHeader h;
    DecoderIter  k_it, v_it;

    /* read header */
    data = intmap_read_header(data, &h);

    if (h.nitems == 0)
        PG_RETURN_NULL();

    /* read keys */
    decoder_iter_init(&k_it, h.key_enc, data);
    decoder_iter_init(&v_it, h.val_enc, data + h.valoff);
    for (uint32_t i = 0; i < h.nitems; ++i) {
        int64_t val = decoder_iter_next(&v_it);

        if (decoder_iter_next(&k_it) == key)
            PG_RETURN_INT64(val);
    }

    /* key's not found */
    PG_RETURN_NULL();
}

static inline const char *encoding_to_str(uint8_t encoding)
{
    switch (encoding) {
        case PLAIN_ENCODING:
            return "none";
        case VARINT_ENCODING:
            return "varint";
        case BITPACK_ENCODING:
            return "bit-pack";
        case VARINT_ENCODING | ZIGZAG_ENCODING:
            return "varint (zig-zag)";
        case BITPACK_ENCODING | ZIGZAG_ENCODING:
            return "bit-pack (zig-zag)";
        case DELTA_ENCODING:
        case DELTA_ENCODING | ZIGZAG_ENCODING:
            return "delta";
        default:
            elog(ERROR, "unexpected encoding");
    }
}

PG_FUNCTION_INFO_V1(intmap_meta);
Datum intmap_meta(PG_FUNCTION_ARGS)
{
    Datum in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint8_t *data = VARDATA(in);
    IntMapHeader h;
    StringInfoData str;

    /* read header */
    data = intmap_read_header(data, &h);

    initStringInfo(&str);
    appendStringInfo(&str, "ver: %u, num: %u, keys encoding: %s, values encoding: %s",
                     h.version,
                     h.nitems,
                     encoding_to_str(h.key_enc),
                     encoding_to_str(h.val_enc));

    PG_RETURN_CSTRING(str.data);
}

PG_FUNCTION_INFO_V1(intarr_in);
Datum intarr_in(PG_FUNCTION_ARGS)
{
    char    *in = PG_GETARG_CSTRING(0);
    int64_t *values;
    int      n;

    parse_intarr(in, &values, &n);

    return create_intarr_internal(values, n);
}

static Datum create_intarr_internal(uint64_t *values, uint32_t n)
{
    uint8_t    *out;
    uint8_t    *data;
    ArrayStats  stats;

    collect_stats(&stats, values, n);

    /*
     * Size estimation includes:
     * - bytea header (4 bytes)
     * - version + encoding (1 byte)
     * - varint encoded number of items (max 5 bytes)
     * - calculated size of encoded data
     */
    out = palloc0(VARHDRSZ + 1 + 5 + stats.best_size);
    data = VARDATA(out);

    /*
     * write the encoding and version
     */
    *data = INTMAP_VERSION << 5;
    *data++ |= stats.best_encoding | (stats.is_signed ? ZIGZAG_ENCODING : 0);

    /* write the number of values */
    data = varint_encode(data, n);

    if (n > 0) {
        /* encode values */
        data = encode_array(data, &stats, values, n);
    }

    SET_VARSIZE(out, data - out);
    return PointerGetDatum(out);
}

PG_FUNCTION_INFO_V1(intarr_out);
Datum intarr_out(PG_FUNCTION_ARGS)
{
    Datum     in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint8_t  *data = VARDATA(in);
    uint64_t  n;
    uint8_t   encoding;
    uint8_t   version;
    DecoderIter it;
    StringInfoData str;

    /* read the encoding and the number of items */
    encoding = 0x0f & *data;
    version = *data++ >> 5;
    data = varint_decode(data, &n);

    if (n == 0)
        PG_RETURN_CSTRING("{}");

    /* iterate through values */
    decoder_iter_init(&it, encoding, data);
    initStringInfo(&str);
    appendStringInfoChar(&str, '{');
    for (uint32_t i = 0; i < n; ++i) {
        appendStringInfo(&str, i == 0 ? "%ld" : ", %ld",
                         decoder_iter_next(&it));
    }
    appendStringInfoChar(&str, '}');

    PG_RETURN_CSTRING(str.data);
}

PG_FUNCTION_INFO_V1(intarr_get_val);
Datum intarr_get_val(PG_FUNCTION_ARGS)
{
    Datum    in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint32_t idx = PG_GETARG_INT64(1);
    uint8_t *data = VARDATA(in);
    uint64_t n;
    uint8_t  encoding;
    uint8_t  version;
    DecoderIter it;
    int64_t  res;

    /* read the encoding and the number of items */
    encoding = 0x0f & *data;
    version = *data++ >> 5;
    data = varint_decode(data, &n);

    /* out of range */
    if (idx < 1 || idx > n)
        PG_RETURN_NULL();

    /* iterate through values */
    decoder_iter_init(&it, encoding, data);
    for (int32_t i = 0; i < idx; ++i)
        res = decoder_iter_next(&it);

    PG_RETURN_INT64(res);
}
