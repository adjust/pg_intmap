![CI](https://github.com/adjust/pg_intmap/workflows/CI/badge.svg) ![experimental](https://img.shields.io/badge/status-experimental-orange)

# pg_intmap

Compressed integer containers.

### intmap

Integer to integer map. Example:

```sql
postgres=# select '10=>125, 20=>250, 30=>0'::intmap->20;
 ?column? 
----------
      250
(1 row)
```

### intarr

Integer array. Example:

```sql
postgres=# select '{100,225,-70}'::intarr->2;
 ?column? 
----------
      225
(1 row)
```
