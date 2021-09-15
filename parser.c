#include <stdlib.h>
#include "postgres.h"


typedef enum {
    IM_MAP_START = 0,
    IM_KEY,
    IM_KV_DELIM,
    IM_VALUE,
    IM_DELIM,
    IM_ARR_START,
    IM_ARR_END
} IMParseState;

static inline const char* parse_int(const char *c, int64_t *out)
{
    char *end;

    *out = strtol(c, &end, 0);

    if (errno == ERANGE)
        elog(ERROR, "integer out of range");
    if (c == end)
        elog(ERROR, "invalid integer");

    return end;
}

static inline void intmap_qsort_internal(int64_t *keys, int64_t *values,
                                         int32_t low, int32_t high)
{
    int64_t mid = keys[(low + high) / 2];
    int32_t i = low, j = high - 1;

    if (low >= high)
        return;

    while (true) {
        int64_t tmp;

        while (keys[i] < mid) i++;
        while (keys[j] > mid) j--;

        if (i >= j)
            break;

        /* swap elements */
        tmp = keys[i];
        keys[i] = keys[j];
        keys[j] = tmp;

        tmp = values[i];
        values[i] = values[j];
        values[j] = tmp;
        i++;
        j--;
    }
    intmap_qsort_internal(keys, values, low, j);
    intmap_qsort_internal(keys, values, j + 1, high);
}

void intmap_qsort(int64_t *keys, int64_t *values, int32_t n)
{
    intmap_qsort_internal(keys, values, 0, n);
}

void parse_intmap(const char *c, int64_t **keys, int64_t **values, int *n)
{
    IMParseState state = IM_MAP_START;
    const char *s = c;
    int         i = 0;

    /* estimate the number of key-value pairs */
    *n = 1;
    while (*s) {
        if (*s == ',')
            (*n)++;
        s++;
    }

    /* allocate keys and values arrays */
    *keys = palloc(sizeof(int64_t) * *n * 2);
    *values = *keys + *n;
    *n = 0;

    /* parse */
    while (*c) {
        /* skip spaces */
        while (isspace(*c))
            c++;

        if (!*c)
            break;

        switch(state) {
            case IM_MAP_START:
            case IM_KEY:
                {
                    int64_t key;

                    c = parse_int(c, &key);
                    (*keys)[i] = key;
                    state = IM_KV_DELIM;
                    break;
                }

            case IM_KV_DELIM:
                if (*c != '=' || *(c + 1) != '>')
                    elog(ERROR, "expected '=>', but found '%s'", c);
                c += 2;
                state = IM_VALUE;
                break;

            case IM_VALUE:
                {
                    int64_t val;

                    c = parse_int(c, &val);
                    (*values)[i++] = val;
                    (*n)++;
                    state = IM_DELIM;
                    break;
                }

            case IM_DELIM:
                if (*c != ',')
                    elog(ERROR, "expected ',', but found '%s'", c);
                c++;
                state = IM_KEY;
                break;

            default:
                Assert(false); /* should never happen */
        }
    }

    if (state != IM_DELIM && state != IM_MAP_START)
        elog(ERROR, "unexpected end of string");

    intmap_qsort(*keys, *values, *n);
}

void parse_intarr(const char *c, int64_t **values, int *n)
{
    IMParseState state = IM_ARR_START;
    const char *s = c;
    int         i = 0;

    /* estimate the number of values */
    *n = 1;
    while (*s) {
        if (*s == ',')
            (*n)++;
        s++;
    }

    /* allocate values array */
    *values = palloc(sizeof(int64_t) * *n);

    while (*c) {
        /* skip spaces */
        while (isspace(*c)) c++;

        if (!*c)
            break;

        switch (state) {
            case IM_ARR_START:
                if (*c != '{')
                    elog(ERROR, "expected '{', but found '%s'", c);
                c++;

                /* skip spaces */
                while (isspace(*c)) c++;

                if (unlikely(*c == '}')) {
                    state = IM_ARR_END;
                    *n = 0;
                    c++;
                } else
                    state = IM_VALUE;
                break;

            case IM_VALUE:
                {
                    int64_t val;

                    c = parse_int(c, &val);
                    (*values)[i++] = val;
                    state = IM_DELIM;
                    break;
                }

            case IM_DELIM:
                switch (*c) {
                    case ',':
                        state = IM_VALUE;
                        break;
                    case '}':
                        state = IM_ARR_END;
                        break;
                    default:
                        elog(ERROR, "expected ',' or '}', but found '%s'", c);
                }
                c++;
                break;

            case IM_ARR_END:
                elog(ERROR, "expected end of array, but found '%s'", c);
                break;

            default:
                Assert(false); /* should never happen */
        }
    }

    if (state != IM_ARR_END)
        elog(ERROR, "unexpected end of string");
}
