-- This table holds the list of recurring data science jobs that are
-- executed by data science pipeline system (Stau). The structure is
-- different from other data-quality tables as the schema is defined
-- by the APScheduler Python package. See Github repo for more,
-- apscheduler/jobstores/sqlalchemy.py#L53
CREATE TABLE IF NOT EXISTS `StauRecurringJobs` (
`id`             varchar(191)    NOT NULL,
`next_run_time`  double          NULL,
`job_state`      blob            NOT NULL,
PRIMARY KEY (`id`),
KEY `nextRunTime` (`next_run_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- This table holds the job history for all jobs executed by
-- by the data science pipeline system (Stau)
CREATE TABLE IF NOT EXISTS `StauQueue` (
`id`                int(11) UNSIGNED  NOT NULL AUTO_INCREMENT,
`jobType`           varchar(255)      NOT NULL,
`status`            varchar(255)      NOT NULL,
`kwargs`            json              NOT NULL,
`dependencies`      json              NOT NULL,
`masterJobId`       int(11) UNSIGNED  DEFAULT NULL,
`createdAt`         datetime          NOT NULL DEFAULT CURRENT_TIMESTAMP,
`updatedAt`         datetime          NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
`execStart`         datetime          DEFAULT NULL,
`execEnd`           datetime          DEFAULT NULL,
`message`           text              DEFAULT NULL,
`execEnvironment`   varchar(255)      DEFAULT NULL,
PRIMARY KEY (`id`),
KEY `jobMasterId` (`masterJobId`),
KEY `jobTypeKey` (`jobType`),
KEY `jobStatusUpdatedAt` (`status`, `updatedAt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- This table holds the execution history on a per car basis
-- for all reports executed by the data science pipeline system (Stau)
CREATE TABLE IF NOT EXISTS `StauLatestExecution` (
`workId`       int(11)              NOT NULL,
`history`      json                 NOT NULL,
`createdAt`    datetime             NOT NULL DEFAULT CURRENT_TIMESTAMP,
`updatedAt`    datetime             NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`workId`),
UNIQUE KEY `workIdKey` (`workId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
