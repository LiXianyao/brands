create table brand_history
(
  id            int auto_increment
    primary key,
  brand_no      varchar(10)  null,
  class_no      int(2)       null,
  apply_date    varchar(8)   null,
  brand_name    varchar(128) null,
  brand_status  int(1)       null,
  insert_status int(1)       null
);

create index brand_history_apply_date_index
  on brand_history (apply_date);

create index brand_history_brand_no_index
  on brand_history (brand_no);

create index brand_history_class_no_index
  on brand_history (class_no);

