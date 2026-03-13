select case when (select count(*)
                  from store_sales ss
                  where ss_quantity >= 1 and ss_quantity <= 20) > 25437
            then (select avg(ss_ext_discount_amt)
                  from store_sales
                  where ss_quantity >= 1 and ss_quantity <= 20)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity >= 1 and ss_quantity <= 20) end bucket1 ,
       case when (select count(*)
                  from store_sales ss
                  where ss_quantity >= 21 and ss_quantity <= 40) > 22746
            then (select avg(ss_ext_discount_amt)
                  from store_sales
                  where ss_quantity >= 21 and ss_quantity <= 40)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity >= 21 and ss_quantity <= 40) end bucket2,
       case when (select count(*)
                  from store_sales ss
                  where ss_quantity >= 41 and ss_quantity <= 60) > 9387
            then (select avg(ss_ext_discount_amt)
                  from store_sales
                  where ss_quantity >= 41 and ss_quantity <= 60)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity >= 41 and ss_quantity <= 60) end bucket3,
       case when (select count(*)
                  from store_sales ss
                  where ss_quantity >= 61 and ss_quantity <= 80) > 10098
            then (select avg(ss_ext_discount_amt)
                  from store_sales
                  where ss_quantity >= 61 and ss_quantity <= 80)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity >= 61 and ss_quantity <= 80) end bucket4,
       case when (select count(*)
                  from store_sales ss
                  where ss_quantity >= 81 and ss_quantity <= 100) > 18213
            then (select avg(ss_ext_discount_amt)
                  from store_sales
                  where ss_quantity >= 81 and ss_quantity <= 100)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity >= 81 and ss_quantity <= 100) end bucket5
from reason
where r_reason_sk = 1
