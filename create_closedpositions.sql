-- Active: 1711729375039@@127.0.0.1@3306@IB
-- [closedPositions] definition

-- Drop table

DROP TABLE IF EXISTS `closedPositions`;

CREATE TABLE `closedPositions` (
	`transactionID` BIGINT DEFAULT 0 NOT NULL,
    `openTransactionID` BIGINT DEFAULT 0,
    `symbol` VARCHAR(255),
	`description` VARCHAR(255),
	`conid` BIGINT DEFAULT 0,
    `assetCategory` VARCHAR(255),
    `openBuySell` VARCHAR(255),
    `openDate` TIMESTAMP,
    `openAmount` DECIMAL(65,4) DEFAULT 0,
    `daysToExpiration` INT DEFAULT 0,
    `openQuantity` INT DEFAULT 0,
    `closeDate` TIMESTAMP,
    `daysInTrade` INT DEFAULT 0,
    `closeAmount` DECIMAL(65,4) DEFAULT 0,
    `closeQuantity` INT DEFAULT 0,
    `closeBuySell` VARCHAR(255),
    `closeResult` DECIMAL(65,4) DEFAULT 0,
    `comment` VARCHAR(255),
	PRIMARY KEY (`transactionID`)
);

CREATE UNIQUE INDEX `SYS_IDX_SYS_CP` ON `closedPositions` (`transactionID`);
CREATE INDEX `CLOSEDPOSITIONS_CONID` ON `closedPositions` (`conid`);
CREATE INDEX `CLOSEDPOSITIONS_TRANSACTIONID` ON `closedPositions` (`transactionID`);
