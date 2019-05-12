create table brand_train_data
(
  brand_name varchar(64)  null,
  brand_no   varchar(12)  null,
  apply_date varchar(8)   null,
  brand_sts  tinyint(1)   null,
  his_name   varchar(64)  null,
  his_no     varchar(12)  null,
  his_date   varchar(8)   null,
  his_sts    tinyint(1)   null,
  class_no   int          null,
  similarity varchar(256) null,
  id         int auto_increment,
  is_similar tinyint(1) null
    primary key
);

create index brand_train_data_apply_date_index
  on brand_train_data (apply_date);

create index brand_train_data_brand_sts_index
  on brand_train_data (brand_sts);

create index brand_train_data_class_no_index
  on brand_train_data (class_no);

