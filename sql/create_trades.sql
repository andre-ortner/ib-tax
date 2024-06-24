-- Active: 1711729375039@@127.0.0.1@3306@IB

-- [trades] definition

-- Drop table

DROP TABLE IF EXISTS `trades`;

CREATE TABLE `trades` (
	`transactionID` BIGINT DEFAULT 0 NOT NULL,
	`accountID` VARCHAR(255) NOT NULL,
	`tradeID` BIGINT DEFAULT 0,
	`description` VARCHAR(255),
	`IBCurrency` VARCHAR(255),
	`conid` BIGINT DEFAULT 0,
	`putCall` VARCHAR(255),
	`tradePrice` DECIMAL(65,4) DEFAULT 0,
	`tradeDate` TIMESTAMP,
	`IBDateTime` TIMESTAMP,
	`strike` VARCHAR(255),
	`expiryDate` TIMESTAMP,
	`action` VARCHAR(255),
	`notes` VARCHAR(255),
	`openClose` VARCHAR(255),
	`quantity` INT DEFAULT 0,
	`buySell` VARCHAR(255),
	`symbol` VARCHAR(255),
	`underlyingSymbol` VARCHAR(255),
	`symbolSort` VARCHAR(255),
	`assetCategory` VARCHAR(255),
	`multiplier` INT DEFAULT 0,
	`transactionType` VARCHAR(255),
	`cost` DECIMAL(18,2) DEFAULT 0,
	`fifoPnlRealized` DECIMAL(18,2) DEFAULT 0,
	`capitalGainsPnl` DECIMAL(18,2) DEFAULT 0,
	`fxPnl` DECIMAL(18,2) DEFAULT 0,
	`fxRateToBase` DECIMAL(18,2) DEFAULT 0,
	`ibCommission` DECIMAL(18,2) DEFAULT 0,
	PRIMARY KEY (`transactionID`)
);

CREATE UNIQUE INDEX `SYS_IDX_SYS_PK_10483_10484` ON `trades` (`transactionID`);
CREATE INDEX `TRADES_CONID` ON `trades` (`conid`);
CREATE INDEX `TRADES_TRADEID` ON `trades` (`accountID`);
CREATE INDEX `TRADES_TRADEID1` ON `trades` (`tradeID`);
CREATE INDEX `TRADES_TRANSACTIONID` ON `trades` (`transactionID`);
