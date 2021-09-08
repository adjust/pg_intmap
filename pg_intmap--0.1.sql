CREATE FUNCTION intmap_in(cstring)
RETURNS intmap
AS 'pg_intmap'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION intmap_out(intmap)
RETURNS cstring
AS 'pg_intmap'
LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE intmap (
    INPUT   = intmap_in,
    OUTPUT  = intmap_out,
    STORAGE = EXTENDED
);

CREATE FUNCTION intmap(int8[], int8[])
RETURNS intmap
AS 'pg_intmap', 'create_intmap'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION intmap_get_val(intmap, int8)
RETURNS int8
AS 'pg_intmap'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR -> (
    leftarg   = intmap,
    rightarg  = int8,
    procedure = intmap_get_val
);

CREATE FUNCTION intmap_meta(intmap)
RETURNS cstring
AS 'pg_intmap'
LANGUAGE C IMMUTABLE STRICT;
