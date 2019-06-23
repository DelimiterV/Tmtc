CREATE TABLE mtstask.tasks (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  guid varchar(36) NOT NULL,
  mstatus varchar(10) NOT NULL,
  mtime varchar(20) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE INDEX UK_tasks_guid (guid)
)
ENGINE = MYISAM
CHARACTER SET utf8
COLLATE utf8_general_ci