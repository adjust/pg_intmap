create extension pg_intmap;

select intmap_meta('85469345=>3, 2=>153, 3=>123');
select intmap_meta('-85469345=>3, 2=>153, 3=>-123');
select '85469345=>3, 2=>153, 3=>123'::intmap;
select '-85469345=>3, 2=>153, 3=>123'::intmap;
select ''::intmap;
select '9223372036854775807=>1'::intmap;
select '9223372036854775808=>1'::intmap;
select '1=>-10496585469345, 2=>10, 3=>1'::intmap->1;
select intmap(array[1, null], array[5, null]);
select intmap(array[1, 2], array[5, 10]);

-- delta encoding
select '10001=>-10001, 10002=>-10002, 10003=>-10003'::intmap;
select '10001=>-10003, 10002=>-10002, 10003=>-10001'::intmap;
select intmap_meta('10001=>-10003, 10002=>-10002, 10003=>-10001'::intmap);

-- arrays
select '{1, 2}'::intarr;
select '{}'::intarr;
