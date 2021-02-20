ALTER TABLE school_statistics.school_enrollment
ADD INDEX `school_id` USING BTREE (NCESS(12)) VISIBLE;

ALTER TABLE school_statistics.school_ccd_data
ADD INDEX `ncessch` USING BTREE (NCESS(12)) VISIBLE;

ALTER TABLE school_statistics.school_enrollment_10
ADD INDEX `school_id` USING BTREE (NCESSCH(12)) VISIBLE;

ALTER TABLE school_statistics.edfacts_ethnic_groups
ADD INDEX `ethgrp` USING BTREE (Abbr(3)) VISIBLE;







