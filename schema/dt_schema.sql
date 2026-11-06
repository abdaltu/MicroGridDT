-- phpMyAdmin SQL Dump
-- version 5.2.1deb3
-- https://www.phpmyadmin.net/
--
-- Generation Time: May 15, 2026 at 02:31 PM
-- Server version: 9.6.1-cloud
-- PHP Version: 8.3.6

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `DT`
--

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY`
--

CREATE TABLE `HISTORY` (
  `ID` bigint DEFAULT NULL,
  `ITEMID` decimal(65,0) NOT NULL,
  `CLOCK` bigint NOT NULL,
  `VALUE` decimal(65,3) DEFAULT NULL,
  `NS` bigint NOT NULL,
  `LIVE` tinyint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `history`
-- (See below for the actual view)
--
CREATE TABLE `history` (
`CLOCK` bigint
,`ID` bigint
,`ITEMID` decimal(65,0)
,`LIVE` tinyint
,`NS` bigint
,`VALUE` decimal(65,3)
);

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_ERROR_EWMA`
--

CREATE TABLE `HISTORY_ERROR_EWMA` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `MODEL` varchar(32) NOT NULL,
  `ERROR` double NOT NULL,
  `ACTUAL` double DEFAULT NULL,
  `FAST_EWMA` double NOT NULL,
  `SLOW_EWMA` double NOT NULL,
  `FINAL_EWMA` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_KAFKA_STAGE`
--

CREATE TABLE `HISTORY_KAFKA_STAGE` (
  `BATCH_ID` varchar(64) NOT NULL,
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE` double NOT NULL,
  `STAGED_AT` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_PRED_DIRECT60`
--

CREATE TABLE `HISTORY_PRED_DIRECT60` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE` double NOT NULL,
  `MODEL` varchar(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_PRED_RECURSIVE`
--

CREATE TABLE `HISTORY_PRED_RECURSIVE` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE` double NOT NULL,
  `MODEL` varchar(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_PRED_SELECTED`
--

CREATE TABLE `HISTORY_PRED_SELECTED` (
  `ITEMID` int UNSIGNED NOT NULL,
  `CLOCK` int UNSIGNED NOT NULL,
  `HARD_MODEL` varchar(20) NOT NULL,
  `HARD_VALUE` float DEFAULT NULL,
  `SOFT_WEIGHTS` json DEFAULT NULL,
  `SOFT_VALUE` float DEFAULT NULL,
  `METHOD` enum('Hard','Soft') NOT NULL,
  `ADAPTIVE_VALUE` float DEFAULT NULL,
  `DECISION_CLOCK` int UNSIGNED NOT NULL,
  `INSERTED_AT` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_SELECTION_LOG`
--

CREATE TABLE `HISTORY_SELECTION_LOG` (
  `ITEMID` int UNSIGNED NOT NULL,
  `CLOCK` int UNSIGNED NOT NULL,
  `ACTUAL` float DEFAULT NULL,
  `HARD_MODEL` varchar(20) NOT NULL,
  `HARD_VALUE` float DEFAULT NULL,
  `SOFT_VALUE` float DEFAULT NULL,
  `SOFT_WEIGHTS` json DEFAULT NULL,
  `METHOD` enum('Hard','Soft') NOT NULL,
  `ADAPTIVE_VALUE` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_SELECTION_STATE`
--

CREATE TABLE `HISTORY_SELECTION_STATE` (
  `ITEMID` int UNSIGNED NOT NULL,
  `LAST_PROCESSED_CLOCK` int UNSIGNED NOT NULL DEFAULT '0',
  `HARD_ACTIVE` varchar(20) NOT NULL,
  `HARD_PENDING` varchar(20) DEFAULT NULL,
  `HARD_STREAK` int NOT NULL DEFAULT '0',
  `HARD_LAST_SWITCH_CLOCK` int UNSIGNED NOT NULL DEFAULT '0',
  `METHOD_ACTIVE` enum('Hard','Soft') NOT NULL DEFAULT 'Hard',
  `METHOD_PENDING` enum('Hard','Soft') DEFAULT NULL,
  `METHOD_STREAK` int NOT NULL DEFAULT '0',
  `METHOD_LAST_SWITCH_CLOCK` int UNSIGNED NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_UINT`
--

CREATE TABLE `HISTORY_UINT` (
  `ID` bigint DEFAULT NULL,
  `ITEMID` bigint DEFAULT NULL,
  `CLOCK` bigint NOT NULL,
  `VALUE` decimal(65,0) DEFAULT NULL,
  `NS` bigint NOT NULL,
  `LIVE` tinyint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `history_uint`
-- (See below for the actual view)
--
CREATE TABLE `history_uint` (
`CLOCK` bigint
,`ID` bigint
,`ITEMID` bigint
,`LIVE` tinyint
,`NS` bigint
,`VALUE` decimal(65,0)
);

-- --------------------------------------------------------

--
-- Table structure for table `HISTORY_VALUE_EWMA`
--

CREATE TABLE `HISTORY_VALUE_EWMA` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `ACTUAL` double DEFAULT NULL,
  `FAST_EWMA` float NOT NULL,
  `SLOW_EWMA` float NOT NULL,
  `FINAL_EWMA` float NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS`
--

CREATE TABLE `TRENDS` (
  `ID` bigint DEFAULT NULL,
  `ITEMID` bigint DEFAULT NULL,
  `CLOCK` bigint NOT NULL,
  `NUM` bigint NOT NULL,
  `VALUE_MIN` double DEFAULT NULL,
  `VALUE_AVG` double DEFAULT NULL,
  `VALUE_MAX` double DEFAULT NULL,
  `LIVE` tinyint DEFAULT NULL,
  `rid` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `trends`
-- (See below for the actual view)
--
CREATE TABLE `trends` (
`CLOCK` bigint
,`ID` bigint
,`ITEMID` bigint
,`LIVE` tinyint
,`NUM` bigint
,`VALUE_AVG` double
,`VALUE_MAX` double
,`VALUE_MIN` double
);

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_ERROR_EWMA`
--

CREATE TABLE `TRENDS_ERROR_EWMA` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `MODEL` varchar(32) NOT NULL,
  `ERROR` double NOT NULL,
  `ACTUAL` double DEFAULT NULL,
  `FAST_EWMA` double NOT NULL,
  `SLOW_EWMA` double NOT NULL,
  `FINAL_EWMA` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_KAFKA_STAGE`
--

CREATE TABLE `TRENDS_KAFKA_STAGE` (
  `BATCH_ID` varchar(64) NOT NULL,
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE_AVG` double NOT NULL,
  `STAGED_AT` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_PRED_DIRECT24`
--

CREATE TABLE `TRENDS_PRED_DIRECT24` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE_AVG` double NOT NULL,
  `MODEL` varchar(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_PRED_RECURSIVE`
--

CREATE TABLE `TRENDS_PRED_RECURSIVE` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `VALUE_AVG` double NOT NULL,
  `MODEL` varchar(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_PRED_SELECTED`
--

CREATE TABLE `TRENDS_PRED_SELECTED` (
  `ITEMID` int UNSIGNED NOT NULL,
  `CLOCK` int UNSIGNED NOT NULL,
  `HARD_MODEL` varchar(20) NOT NULL,
  `HARD_VALUE` float DEFAULT NULL,
  `SOFT_WEIGHTS` json DEFAULT NULL,
  `SOFT_VALUE` float DEFAULT NULL,
  `METHOD` enum('Hard','Soft') NOT NULL,
  `ADAPTIVE_VALUE` float DEFAULT NULL,
  `DECISION_CLOCK` int UNSIGNED NOT NULL,
  `INSERTED_AT` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_SELECTION_LOG`
--

CREATE TABLE `TRENDS_SELECTION_LOG` (
  `ITEMID` int UNSIGNED NOT NULL,
  `CLOCK` int UNSIGNED NOT NULL,
  `ACTUAL` float DEFAULT NULL,
  `HARD_MODEL` varchar(20) NOT NULL,
  `HARD_VALUE` float DEFAULT NULL,
  `SOFT_VALUE` float DEFAULT NULL,
  `SOFT_WEIGHTS` json DEFAULT NULL,
  `METHOD` enum('Hard','Soft') NOT NULL,
  `ADAPTIVE_VALUE` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_SELECTION_STATE`
--

CREATE TABLE `TRENDS_SELECTION_STATE` (
  `ITEMID` int UNSIGNED NOT NULL,
  `LAST_PROCESSED_CLOCK` int UNSIGNED NOT NULL DEFAULT '0',
  `HARD_ACTIVE` varchar(20) NOT NULL,
  `HARD_PENDING` varchar(20) DEFAULT NULL,
  `HARD_STREAK` int NOT NULL DEFAULT '0',
  `HARD_LAST_SWITCH_CLOCK` int UNSIGNED NOT NULL DEFAULT '0',
  `METHOD_ACTIVE` enum('Hard','Soft') NOT NULL DEFAULT 'Hard',
  `METHOD_PENDING` enum('Hard','Soft') DEFAULT NULL,
  `METHOD_STREAK` int NOT NULL DEFAULT '0',
  `METHOD_LAST_SWITCH_CLOCK` int UNSIGNED NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_UINT`
--

CREATE TABLE `TRENDS_UINT` (
  `ID` bigint DEFAULT NULL,
  `ITEMID` bigint DEFAULT NULL,
  `CLOCK` bigint NOT NULL,
  `NUM` bigint NOT NULL,
  `VALUE_MIN` decimal(65,0) NOT NULL,
  `VALUE_AVG` decimal(65,0) NOT NULL,
  `VALUE_MAX` decimal(65,0) NOT NULL,
  `LIVE` tinyint DEFAULT NULL,
  `rid` int NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `trends_uint`
-- (See below for the actual view)
--
CREATE TABLE `trends_uint` (
`CLOCK` bigint
,`ID` bigint
,`ITEMID` bigint
,`LIVE` tinyint
,`NUM` bigint
,`VALUE_AVG` decimal(65,0)
,`VALUE_MAX` decimal(65,0)
,`VALUE_MIN` decimal(65,0)
);

-- --------------------------------------------------------

--
-- Table structure for table `TRENDS_VALUE_EWMA`
--

CREATE TABLE `TRENDS_VALUE_EWMA` (
  `ITEMID` int NOT NULL,
  `CLOCK` int NOT NULL,
  `ACTUAL` double DEFAULT NULL,
  `FAST_EWMA` double NOT NULL,
  `SLOW_EWMA` double NOT NULL,
  `FINAL_EWMA` double NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Structure for view `history`
--
DROP TABLE IF EXISTS `history`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `history`  AS SELECT `HISTORY`.`ID` AS `ID`, `HISTORY`.`ITEMID` AS `ITEMID`, `HISTORY`.`CLOCK` AS `CLOCK`, `HISTORY`.`VALUE` AS `VALUE`, `HISTORY`.`NS` AS `NS`, `HISTORY`.`LIVE` AS `LIVE` FROM `HISTORY` ;

-- --------------------------------------------------------

--
-- Structure for view `history_uint`
--
DROP TABLE IF EXISTS `history_uint`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `history_uint`  AS SELECT `HISTORY_UINT`.`ID` AS `ID`, `HISTORY_UINT`.`ITEMID` AS `ITEMID`, `HISTORY_UINT`.`CLOCK` AS `CLOCK`, `HISTORY_UINT`.`VALUE` AS `VALUE`, `HISTORY_UINT`.`NS` AS `NS`, `HISTORY_UINT`.`LIVE` AS `LIVE` FROM `HISTORY_UINT` ;

-- --------------------------------------------------------

--
-- Structure for view `trends`
--
DROP TABLE IF EXISTS `trends`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `trends`  AS SELECT `TRENDS`.`ID` AS `ID`, `TRENDS`.`ITEMID` AS `ITEMID`, `TRENDS`.`CLOCK` AS `CLOCK`, `TRENDS`.`NUM` AS `NUM`, `TRENDS`.`VALUE_MIN` AS `VALUE_MIN`, `TRENDS`.`VALUE_AVG` AS `VALUE_AVG`, `TRENDS`.`VALUE_MAX` AS `VALUE_MAX`, `TRENDS`.`LIVE` AS `LIVE` FROM `TRENDS` ;

-- --------------------------------------------------------

--
-- Structure for view `trends_uint`
--
DROP TABLE IF EXISTS `trends_uint`;

CREATE ALGORITHM=UNDEFINED SQL SECURITY INVOKER VIEW `trends_uint`  AS SELECT `TRENDS_UINT`.`ID` AS `ID`, `TRENDS_UINT`.`ITEMID` AS `ITEMID`, `TRENDS_UINT`.`CLOCK` AS `CLOCK`, `TRENDS_UINT`.`NUM` AS `NUM`, `TRENDS_UINT`.`VALUE_MIN` AS `VALUE_MIN`, `TRENDS_UINT`.`VALUE_AVG` AS `VALUE_AVG`, `TRENDS_UINT`.`VALUE_MAX` AS `VALUE_MAX`, `TRENDS_UINT`.`LIVE` AS `LIVE` FROM `TRENDS_UINT` ;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `HISTORY`
--
ALTER TABLE `HISTORY`
  ADD KEY `ITEMID` (`ITEMID`,`CLOCK`),
  ADD KEY `clock` (`CLOCK`);

--
-- Indexes for table `HISTORY_ERROR_EWMA`
--
ALTER TABLE `HISTORY_ERROR_EWMA`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `HISTORY_KAFKA_STAGE`
--
ALTER TABLE `HISTORY_KAFKA_STAGE`
  ADD PRIMARY KEY (`BATCH_ID`,`ITEMID`,`CLOCK`),
  ADD KEY `idx_staged_at` (`STAGED_AT`);

--
-- Indexes for table `HISTORY_PRED_DIRECT60`
--
ALTER TABLE `HISTORY_PRED_DIRECT60`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `HISTORY_PRED_RECURSIVE`
--
ALTER TABLE `HISTORY_PRED_RECURSIVE`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `HISTORY_PRED_SELECTED`
--
ALTER TABLE `HISTORY_PRED_SELECTED`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`),
  ADD KEY `idx_decision` (`ITEMID`,`DECISION_CLOCK`);

--
-- Indexes for table `HISTORY_SELECTION_LOG`
--
ALTER TABLE `HISTORY_SELECTION_LOG`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`);

--
-- Indexes for table `HISTORY_SELECTION_STATE`
--
ALTER TABLE `HISTORY_SELECTION_STATE`
  ADD PRIMARY KEY (`ITEMID`);

--
-- Indexes for table `HISTORY_UINT`
--
ALTER TABLE `HISTORY_UINT`
  ADD KEY `ITEMID` (`ITEMID`,`CLOCK`);

--
-- Indexes for table `HISTORY_VALUE_EWMA`
--
ALTER TABLE `HISTORY_VALUE_EWMA`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`);

--
-- Indexes for table `TRENDS`
--
ALTER TABLE `TRENDS`
  ADD PRIMARY KEY (`rid`),
  ADD KEY `ITEMID` (`ITEMID`,`CLOCK`);

--
-- Indexes for table `TRENDS_ERROR_EWMA`
--
ALTER TABLE `TRENDS_ERROR_EWMA`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `TRENDS_KAFKA_STAGE`
--
ALTER TABLE `TRENDS_KAFKA_STAGE`
  ADD PRIMARY KEY (`BATCH_ID`,`ITEMID`,`CLOCK`),
  ADD KEY `idx_staged_at` (`STAGED_AT`);

--
-- Indexes for table `TRENDS_PRED_DIRECT24`
--
ALTER TABLE `TRENDS_PRED_DIRECT24`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `TRENDS_PRED_RECURSIVE`
--
ALTER TABLE `TRENDS_PRED_RECURSIVE`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`,`MODEL`);

--
-- Indexes for table `TRENDS_PRED_SELECTED`
--
ALTER TABLE `TRENDS_PRED_SELECTED`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`),
  ADD KEY `idx_decision` (`ITEMID`,`DECISION_CLOCK`);

--
-- Indexes for table `TRENDS_SELECTION_LOG`
--
ALTER TABLE `TRENDS_SELECTION_LOG`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`);

--
-- Indexes for table `TRENDS_SELECTION_STATE`
--
ALTER TABLE `TRENDS_SELECTION_STATE`
  ADD PRIMARY KEY (`ITEMID`);

--
-- Indexes for table `TRENDS_UINT`
--
ALTER TABLE `TRENDS_UINT`
  ADD PRIMARY KEY (`rid`);

--
-- Indexes for table `TRENDS_VALUE_EWMA`
--
ALTER TABLE `TRENDS_VALUE_EWMA`
  ADD PRIMARY KEY (`ITEMID`,`CLOCK`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `TRENDS`
--
ALTER TABLE `TRENDS`
  MODIFY `rid` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `TRENDS_UINT`
--
ALTER TABLE `TRENDS_UINT`
  MODIFY `rid` int NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
