-- Schema for web3_mvp (database will be selected by setup script)

-- 幣別
CREATE TABLE IF NOT EXISTS coins (
  id INT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL UNIQUE,
  coingecko_id VARCHAR(64) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1
);

-- K 線（日/小時皆可，先用 1d）
CREATE TABLE IF NOT EXISTS candles (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  ts BIGINT NOT NULL,
  timeframe ENUM('1h','4h','1d') NOT NULL DEFAULT '1d',
  open DECIMAL(24,8) NOT NULL,
  high DECIMAL(24,8) NOT NULL,
  low DECIMAL(24,8) NOT NULL,
  close DECIMAL(24,8) NOT NULL,
  volume DECIMAL(28,8) NOT NULL,
  UNIQUE KEY uniq_symbol_ts_interval (symbol, ts, timeframe),
  INDEX idx_symbol_ts (symbol, ts),
  CONSTRAINT fk_candles_symbol CHECK (symbol IN ('BTC','ETH','ADA','CRO','PEPE','LUNC','TRX','SNEK'))
);

-- 鏈上彙整（MVP 可先填 mock，再接真實）
CREATE TABLE IF NOT EXISTS onchain_daily (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  ts BIGINT NOT NULL,
  active_addr BIGINT DEFAULT NULL,
  tx_count BIGINT DEFAULT NULL,
  gas_used BIGINT DEFAULT NULL,
  stable_netflow DECIMAL(28,8) DEFAULT NULL,
  whale_tx BIGINT DEFAULT NULL,
  UNIQUE KEY uniq_symbol_ts (symbol, ts),
  INDEX idx_onchain_symbol_ts (symbol, ts),
  CONSTRAINT fk_onchain_symbol CHECK (symbol IN ('BTC','ETH','ADA','CRO','PEPE','LUNC','TRX','SNEK'))
);

-- 機率輸出
CREATE TABLE IF NOT EXISTS prob_signal (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  ts BIGINT NOT NULL,
  horizon ENUM('4h','24h','1d') NOT NULL DEFAULT '1d',
  p_up DECIMAL(6,4) NOT NULL,
  p_down DECIMAL(6,4) NOT NULL,
  verdict ENUM('bull','neutral','bear') NOT NULL,
  features_json JSON NULL,
  UNIQUE KEY uniq_symbol_ts_horizon (symbol, ts, horizon),
  INDEX idx_prob_symbol_ts (symbol, ts),
  CONSTRAINT fk_prob_symbol CHECK (symbol IN ('BTC','ETH','ADA','CRO','PEPE','LUNC','TRX','SNEK'))
);

-- 支撐/壓力價帶
CREATE TABLE IF NOT EXISTS levels (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  ts BIGINT NOT NULL,
  method ENUM('pivot','swing','vbp') NOT NULL,
  bands_json JSON NOT NULL,
  INDEX idx_levels_symbol_ts (symbol, ts, method),
  CONSTRAINT fk_levels_symbol CHECK (symbol IN ('BTC','ETH','ADA','CRO','PEPE','LUNC','TRX','SNEK'))
);

-- 可選：回測彙整
CREATE TABLE IF NOT EXISTS backtest_result (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  strategy VARCHAR(32) NOT NULL,
  window_days INT NOT NULL,
  metrics_json JSON NOT NULL,
  curve_json JSON NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_bt_symbol (symbol, strategy)
);

-- 初始幣別種子
INSERT IGNORE INTO coins (symbol, coingecko_id) VALUES
('BTC','bitcoin'),
('ETH','ethereum'),
('ADA','cardano'),
('CRO','cronos'),
('PEPE','pepe'),
('LUNC','terra-luna'),
('TRX','tron'),
('SNEK','snek');