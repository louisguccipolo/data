select 'hash_contract' as check_name
where {{ hash_key(["' a '", "'b'"]) }} <> md5('A||B')::char(32)
   or {{ hash_key(["null::text", "' '"]) }} <> repeat('0',32)::char(32)
   or {{ hash_key(["null::text", "'b'"]) }} <> md5('||B')::char(32)
   or {{ hashdiff(["null::text", "' a '"]) }} <> md5('|| a ')::char(32)
   or {{ safe_cast("'bad'", 'integer') }} is not null
   or {{ safe_cast("'1e100'", 'numeric(18,2)') }} is not null
   or {{ safe_cast("' 2 '", 'integer') }} <> 2
