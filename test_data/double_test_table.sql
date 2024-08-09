CREATE TABLE `float_sample` (
  `text` char(20) NOT NULL,
  `single_f` float DEFAULT NULL,
  `double_f` double DEFAULT NULL,
  PRIMARY KEY (`text`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
