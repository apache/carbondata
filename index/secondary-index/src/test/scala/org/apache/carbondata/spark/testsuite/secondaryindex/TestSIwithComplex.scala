package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.test.util.QueryTest

class TestSIwithComplex extends QueryTest{

  test("test complex") {
        sql("drop table if exists complextable")
        sql("create table complextable (country array<string>, name string) stored as carbondata")
        sql("insert into complextable select array('china', 'us'), 'b'")
        sql("insert into complextable select array('pak'), 'v'")
        sql("insert into complextable select array('china'), 'f'")
        sql("insert into complextable select array('india'),'g'")
        sql("create index index_1 on table complextable(country) as 'carbondata'")
        sql("select * from index_1").show(false)
    sql(" select * from complextable where country[0]='china'").show(false)
//    sql("explain select * from complextable where country[0]='china'").show(false)

  }

  test("test complex2") {
//    sql("drop table if exists complextable")
//    sql("create table complextable (country array<string>, name string, id string) stored as carbondata")
//    sql("insert into complextable select array('china', 'us'), 'b', 'c'")
//    sql("insert into complextable select array('pak'), 'v', 'd'")
//    sql("insert into complextable select array('china'), 'f', 'e'")
//    sql("insert into complextable select array('india'),'g', 'f'")
//    sql("create index index_1 on table complextable(id, country) as 'carbondata'")
//    sql("select * from index_1").show(false)
    sql(" select * from complextable where country[0]='china'").show(false)
        sql("explain select * from complextable where country[0]='china'").show(false)

  }

  test("test") {
        sql("drop table if exists complextable")
        sql("create table complextable (name string, id string, country string) stored as carbondata")
        sql("insert into complextable select 'xx', '1', 'china'")
    //    sql("insert into complextable select 'xx', '1', 'ind'")
    //    sql("insert into complextable select 'xx', '1', 'china'")
    //    sql("alter table complextable compact 'major'")

    //    sql("insert into complextable select 'xx', '2', 'us'")
    //    sql("insert into complextable select 'xx', '1', 'pak'")
    //    sql("insert into complextable select 'xx', '3', 'china'")
    //    sql("insert into complextable select 'xx', '4', 'india'")
        sql("create index index_1 on table complextable(country) as 'carbondata'")
    //    sql("select * from index_1").show(false)
//    sql("select * from complextable where country='china'").show(false)

  }

  test("test complex ctas") {
    sql("drop table if exists complextable")
    sql("create table complextable (name string, id string, country array<string>) stored as carbondata")
    sql("insert into complextable select 'xx', '1', array('china', 'can')")
    sql("drop table if exists xxx")
    sql("create table xxx stored as carbondata as select country from complextable")
  }


}
