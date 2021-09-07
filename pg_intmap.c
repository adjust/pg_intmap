#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "utils/array.h"

#include "encodings.h"


PG_MODULE_MAGIC;


#define ENCODING_PLAIN      0
#define ENCODING_VARINT     1
#define ENCODING_BITPACK    2
#define ENCODING_ZIGZAG     8


typedef struct
{
    /* uint8 version; TODO */
    uint32 nitems;
    uint32 valoff;  /* values offset */
} IntMapHeader;

typedef struct
{
    int64_t min;
    int64_t max;
    uint32_t varint_bytes;  /* bytes required to store all values in varint */
    uint32_t bitpack_bytes; /* bytes required to store all values in bitpack */
    uint8_t num_bits;       /* sufficient number of bits per value to encode
                               using bit packing */
    uint8_t best_encoding;
    bool    use_zigzag;     /* use zigzag encoding to encode signed values */
} ArrayStats;


void parse_intmap(const char *c, int64_t **keys, int64_t **values, int *n);
static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n);


static void collect_stats(ArrayStats *stats, int64_t *vals, int n)
{
    /* TODO: account for zigzag encoding */

    int64_t min = PG_INT64_MAX;
    int64_t max = PG_INT64_MIN;
    uint64_t varint_bytes = 0;

    for (int i = 0; i < n; ++i) {
        min = min < vals[i] ? min : vals[i];
        max = max > vals[i] ? max : vals[i];
        varint_bytes += ((sizeof(int64_t) << 3) - __builtin_clzl(vals[i]) + 7) / 7;
    }

    stats->varint_bytes = varint_bytes;
    stats->num_bits = (sizeof(int64_t) << 3) - __builtin_clzl(max);

    /* number of bits / 8 + 1 byte for bits length encoding */
    stats->bitpack_bytes = ((n * stats->num_bits + 7) >> 3) + 1;

    stats->min = min;
    stats->max = max;

    stats->best_encoding = stats->varint_bytes < stats->bitpack_bytes ? \
        ENCODING_VARINT : ENCODING_BITPACK;
    stats->use_zigzag = min < 0;
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

static inline uint8_t *write_encodings(uint8_t *buf,
                                       uint8_t keys_encoding,
                                       uint8_t vals_encoding)
{
    *buf = keys_encoding << 4 | (vals_encoding & 0xF);
    return ++buf;
}

static inline uint8_t *read_encodings(uint8_t *buf,
                                      uint8_t *keys_encoding,
                                      uint8_t *vals_encoding)
{
    *keys_encoding = *buf >> 4;
    *vals_encoding = *buf & 0xF;
    return ++buf;
}

static inline uint8_t *encode_array(uint8_t *buf, ArrayStats *stats,
                                    int64_t *vals, uint32_t n)
{
    if (stats->use_zigzag)
        for (uint32_t i = 0; i < n; ++i)
            vals[i] = (int64_t) zigzag_encode(vals[i]);

    switch (stats->best_encoding) {
        case ENCODING_VARINT:
            for (uint32_t i = 0; i < n; ++i)
                buf = varint_encode(buf, vals[i]);
            break;
        case ENCODING_BITPACK:
            buf = write_num_bits(buf, stats->num_bits);
            buf = bitpack_encode(buf, vals, n, stats->num_bits);
            break;
        default:
            elog(ERROR, "unexpected encoding");
    }

    return buf;
}

static inline uint8_t *decode_array(uint8_t *buf, uint8_t encoding,
                                    uint64_t *vals, uint32_t n)
{
    switch (encoding & 0x7)
    {
        case ENCODING_VARINT:
            for (uint32_t i = 0; i < n; ++i)
                buf = varint_decode(buf, &vals[i]);
            break;
        case ENCODING_BITPACK:
            {
                uint8_t num_bits;

                buf = read_num_bits(buf, &num_bits);
                buf = bitpack_decode(buf, vals, n, num_bits);
                break;
            }
        default:
            elog(ERROR, "unsupported encoding");
    }

    if (encoding & ENCODING_ZIGZAG)
        for (uint32_t i = 0; i < n; ++i)
            vals[i] = zigzag_decode(vals[i]);

    return buf;
}

PG_FUNCTION_INFO_V1(intmap_out);
Datum intmap_out(PG_FUNCTION_ARGS)
{
    Datum in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint64_t *keys;
    uint64_t *values;
    uint8_t *data = VARDATA(in);
    uint64_t n;
    uint8_t keys_encoding, vals_encoding;
    StringInfoData str;

    /* read the number of items */
    data = varint_decode(data, &n);

    /* allocate memory for keys and values in a single palloc */
    keys = palloc(sizeof(uint64_t) * n * 2);
    values = keys + n;

    data = read_encodings(data, &keys_encoding, &vals_encoding);

    /* read keys and values */
    data = decode_array(data, keys_encoding, keys, n);
    data = decode_array(data, vals_encoding, values, n);

    initStringInfo(&str);
    for (uint32_t i = 0; i < n; ++i)
        appendStringInfo(&str, i == 0 ? "%ld=>%ld" : ", %ld=>%ld", keys[i], values[i]);

    PG_RETURN_CSTRING(str.data);
}

static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n)
{
    uint8_t    *out;
    uint8_t    *data;
    ArrayStats key_stats, val_stats;

    /* TODO: estimate size */
    out = palloc0(VARHDRSZ + 5 + sizeof(uint64_t) * n * 2);
    data = VARDATA(out);

    /* 
     * Write header
     * TODO: add version, values offset
     */
    data = varint_encode(data, n);
    collect_stats(&key_stats, keys, n);
    collect_stats(&val_stats, values, n);

    data = write_encodings(data,
       key_stats.best_encoding | (key_stats.use_zigzag ? ENCODING_ZIGZAG : 0),
       val_stats.best_encoding | (val_stats.use_zigzag ? ENCODING_ZIGZAG : 0));

    /* Encode keys and values */
    data = encode_array(data, &key_stats, keys, n);
    data = encode_array(data, &val_stats, values, n);

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

    return create_intmap_internal(keys, values, nkeys);
}


PG_FUNCTION_INFO_V1(intmap_get_val);
Datum intmap_get_val(PG_FUNCTION_ARGS)
{
    Datum    in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint64_t key = DatumGetUInt64(PG_GETARG_DATUM(1));
    uint8_t *data = VARDATA(in);
    int32_t  found_idx = -1;
    uint64_t n;

    /* read the number of items */
    data = varint_decode(data, &n);

    /* read keys */
    for (uint32_t i = 0; i < n; ++i)
    {
        uint64_t cur_key;

        data = varint_decode(data, &cur_key);
        if (cur_key == key) {
            found_idx = i;
        }
    }

    if (found_idx >= 0) {
        uint64_t res;

        /* read values */
        for (int32_t i = 0; i <= found_idx; ++i)
            data = varint_decode(data, &res);

        PG_RETURN_UINT64(res);
    }

    /* key's not found */
    PG_RETURN_NULL();
}
