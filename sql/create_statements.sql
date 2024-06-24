-- Active: 1711729375039@@127.0.0.1@3306@IB

-- [statements] definition

-- Drop table

DROP TABLE IF EXISTS `statements`;

CREATE TABLE `statements` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `transactionID` BIGINT DEFAULT 0 NOT NULL,
    `accountID` VARCHAR(255) NOT NULL,
    `datum` TIMESTAMP,
    `amount` DECIMAL(65,4) DEFAULT 0,
    `IBcurrency` VARCHAR(255),
    `levelOfDetail` VARCHAR(255),
    `tradeID` BIGINT DEFAULT 0,
    `balance` DECIMAL(65,4) DEFAULT 0,
    `description` VARCHAR(255),
    `activityCode` VARCHAR(255),
    `activityDescription` VARCHAR(255),
    `symbol` VARCHAR(255),
    `underlyingSymbol` VARCHAR(255),
    `txInfo` VARCHAR(255),
    `opInfo` VARCHAR(255),
    PRIMARY KEY (`id`)
);


CREATE INDEX `STATEMENTS_ACTIVITYCODE` ON `statements` (`activityCode`);
CREATE INDEX `STATEMENTS_ID` ON `statements` (`id`);
CREATE INDEX `STATEMENTS_TRADEID` ON `statements` (`accountID`);
CREATE INDEX `STATEMENTS_TRADEID1` ON `statements` (`tradeID`);
CREATE INDEX `STATEMENTS_TRANSACTIONID` ON `statements` (`transactionID`);
CREATE UNIQUE INDEX `SYS_IDX_SYS_PK_10470_10471` ON `statements` (`id`);
