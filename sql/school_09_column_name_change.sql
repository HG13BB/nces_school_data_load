ALTER TABLE `school_statistics`.`school_enrollment` 
CHANGE COLUMN `NCESS` `NCESSCH` TEXT NULL DEFAULT NULL ;

ALTER TABLE `school_statistics`.`school_ccd_data`
CHANGE COLUMN `NCESS` `NCESSCH` TEXT NULL DEFAULT NULL ;