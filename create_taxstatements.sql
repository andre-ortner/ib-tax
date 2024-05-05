-- Active: 1711729375039@@127.0.0.1@3306@IB
-- [closedPositions] definition

-- Drop table

DROP TABLE IF EXISTS `taxStatements`;

CREATE TABLE `taxStatements` (
	`transactionID` BIGINT DEFAULT 0 NOT NULL,
    `symbol` VARCHAR(255),
    `underlyingSymbol` VARCHAR(255),
    `conid` BIGINT DEFAULT 0,
	`description` VARCHAR(255),
    `activityDescription` VARCHAR(255),
	`assetCategory` VARCHAR(255),
    `openClose` VARCHAR(255),
    `tradeDate` TIMESTAMP,
    `taxYear` INT DEFAULT 0,
    `buySell` VARCHAR(255),
    `putCall` VARCHAR(255),
    `baseAmount` DECIMAL(65,4) DEFAULT 0,
    `baseCurrency` VARCHAR(255),
    `baseBalance` DECIMAL(65,4) DEFAULT 0,
    `fifoPnlRealized` DECIMAL(65,4) DEFAULT 0,
    `taxFiFoResult` DECIMAL(65,4) DEFAULT 0,
    `taxAmountStillhalterGewinn` DECIMAL(65,4) DEFAULT 0,
    `taxAmountStillhalterVerlust` DECIMAL(65,4) DEFAULT 0,
    `taxAmountTerminGewinn` DECIMAL(65,4) DEFAULT 0,
    `taxAmountTerminVerlust` DECIMAL(65,4) DEFAULT 0,
    `taxAmountAktienGewinn` DECIMAL(65,4) DEFAULT 0,
    `taxAmountAktienVerlust` DECIMAL(65,4) DEFAULT 0,	
    `quantity` INT DEFAULT 0,
    `comment` VARCHAR(255),
    `datensatz` VARCHAR(255),
    `fxRateToBase` DECIMAL(65,4) DEFAULT 0,
    `action` VARCHAR(255),
	PRIMARY KEY (`transactionID`)
);



CREATE UNIQUE INDEX `SYS_IDX_SYS_TS` ON `taxStatements` (`transactionID`);
CREATE INDEX `TAXSTATEMENTS_CONID` ON `taxStatements` (`conid`);
CREATE INDEX `TAXSTATEMENTS_TRANSACTIONID` ON `taxStatements` (`transactionID`);
