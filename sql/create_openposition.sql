-- Active: 1711729375039@@127.0.0.1@3306@IB
-- [openPositions] definition

-- Drop table

DROP TABLE IF EXISTS `openPositions`;

CREATE TABLE `openPositions` (
	`transactionID` BIGINT DEFAULT 0 NOT NULL,
    `symbol` VARCHAR(255),
	`description` VARCHAR(255),
	`conid` BIGINT DEFAULT 0,
	`amount` INT DEFAULT 0,
    `quantity` INT DEFAULT 0,
    `buySell` VARCHAR(255),
    `assetCategory` VARCHAR(255),
	`combo` VARCHAR(255),
	`daysToExpiration` INT DEFAULT 0,
	PRIMARY KEY (`transactionID`)
);

CREATE UNIQUE INDEX `SYS_IDX_SYS_OP` ON `openPositions` (`transactionID`);
CREATE INDEX `OPENPOSITIONS_CONID` ON `openPositions` (`conid`);
CREATE INDEX `OPENPOSITIONS_TRANSACTIONID` ON `openPositions` (`transactionID`);
