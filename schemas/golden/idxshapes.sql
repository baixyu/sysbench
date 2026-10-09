-- 索引形状：前缀 / 降序 / 函数 / 多值 / INVISIBLE / UNIQUE（ADR-0024 起全部进 manifest）
CREATE TABLE perf_doctor_golden.idxshapes (
  id INT NOT NULL,
  v VARCHAR(200) NOT NULL,
  k INT NOT NULL,
  j JSON NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_v (v),
  KEY k_prefix (v(10)),
  KEY k_desc (k DESC),
  KEY k_func ((k + 1)),
  KEY k_multi ((CAST(j->>'$.a' AS UNSIGNED ARRAY))),
  KEY k_invisible (v) INVISIBLE
) ENGINE=InnoDB
