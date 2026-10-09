-- 行长表（§5 的平均行长口径）：必需列 + VARCHAR 填充列
CREATE TABLE perf_doctor_golden.rowlen (
  id INT NOT NULL,
  k INT NOT NULL,
  pad VARCHAR(392) NOT NULL,
  PRIMARY KEY (id),
  KEY k_k (k)
) ENGINE=InnoDB
