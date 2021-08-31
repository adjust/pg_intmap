#include "postgres.h"
#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "utils/array.h"

#include "encodings.h"


PG_MODULE_MAGIC;

typedef struct
{
    /* uint8 version; TODO */
    uint32 nitems;
    uint32 valoff;  /* values offset */
} IntMapHeader;

void parse_intmap(const char *c, int64_t **keys, int64_t **values, int *n);
static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n);


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
    Datum in = PointerGetDatum(PG_DETOAST_DATUM(PG_GETARG_DATUM(0)));
    uint64_t *keys;
    uint64_t *values;
    uint8_t *data = VARDATA(in);
    uint64_t n;
    StringInfoData str;

    /* read the number of items */
    data = varint_decode(data, &n);

    /* allocate memory for keys and values in a single palloc */
    keys = palloc(sizeof(uint64_t) * n * 2);
    values = keys + n;

    /* read keys */
    for (uint32_t i = 0; i < n; ++i)
        data = varint_decode(data, &keys[i]);

    /* read values */
    for (uint32_t i = 0; i < n; ++i)
        data = varint_decode(data, &values[i]);

    initStringInfo(&str);
    for (uint32_t i = 0; i < n; ++i)
        appendStringInfo(&str, i == 0 ? "%ld=>%ld" : ", %ld=>%ld", keys[i], values[i]);

    PG_RETURN_CSTRING(str.data);
}


static Datum create_intmap_internal(uint64_t *keys, uint64_t *values, uint32_t n)
{
    uint8_t    *out;
    uint8_t    *data;

    /* TODO: estimate size */
    out = palloc0(VARHDRSZ + 5 + sizeof(uint64_t) * n * 2);
    data = VARDATA(out);

    /* 
     * Write header
     * TODO: add version, values offset, used encodings
     */
    data = varint_encode(data, n);

    /* Encode keys */
    for (uint32_t i = 0; i < n; ++i)
        data = varint_encode(data, keys[i]);

    /* Encode values */
    for (uint32_t i = 0; i < n; ++i)
        data = varint_encode(data, values[i]);

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
