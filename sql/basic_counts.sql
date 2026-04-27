# get a count of the total number of entries
select count(*) from dup_hostids;

# check the percentage
select 
    substring(hostid, 1, 1) as first_char,
    count(*) as cnt,
    round(count(*) * 100.0 / 3268577207, 2) as pct
from dup_hostids
group by first_char
order by first_char;

# count the number of repeated hostids
select count (distinct hostid) from dup_hostids:x

# get their entropy as well
create materialized view if not exists list_of_dup_ids as 
select hostid, max(entropy) as entropy
from dup_hostids
group by hostid;


# patterns
# 00000000%
# 00010000%
# 00010001%
# 00010002%
# 00020001%
# 00020002%

substring(hostid from 7 for 5) = 'ff0fe'
# 002416ff0fe%
# 861c70ff0fe%
# 8627b6ff0fe%
# 8687ffff0fe%
# 7e6a60ff0fe%
# c22d2eff0fe%
# 867adfff0fe%
# 8ee117ff0fe%
# 8eeefdff0fe%
# 9a9d39ff0fe%
# 80000%
# only contains numbers?


# not gonna filter out these? not sure? 
# 80000?