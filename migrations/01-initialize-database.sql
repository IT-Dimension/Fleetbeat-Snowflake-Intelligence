-- FleetBeat Snowflake Demo Schema & Simulation
CREATE OR REPLACE DATABASE FLEETBEAT_DB;
CREATE OR REPLACE SCHEMA FLEETBEAT_DB.AI_OPTIMIZATION;
USE SCHEMA FLEETBEAT_DB.AI_OPTIMIZATION;

-- 1. Tables
CREATE OR REPLACE TABLE FLEET_TELEMETRY (
  record_id STRING,
  vehicle_id STRING,
  driver_id STRING,
  timestamp TIMESTAMP,
  gps_lat FLOAT,
  gps_lon FLOAT,
  speed FLOAT,
  idle_time_minutes FLOAT,
  fuel_used_liters FLOAT,
  route_id STRING,
  weather STRING,
  distance_km FLOAT
);

CREATE OR REPLACE TABLE AI_PREDICTIONS (
  prediction_id STRING,
  driver_id STRING,
  vehicle_id STRING,
  route_id STRING,
  prediction_time TIMESTAMP,
  predicted_roi FLOAT,
  predicted_ltv FLOAT,
  predicted_sat FLOAT,
  risk_score FLOAT,
  ump_version STRING,
  action_recommended STRING,
  ump_compliant BOOLEAN
);

CREATE OR REPLACE TABLE REALIZED_OUTCOMES (
  outcome_id STRING,
  prediction_id STRING,
  verification_window_days INT,
  realized_roi FLOAT,
  realized_sat FLOAT,
  realized_ltv FLOAT,
  verified BOOLEAN,
  timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE REWARDS_LEDGER (
  reward_id STRING,
  driver_id STRING,
  prediction_id STRING,
  token_amount FLOAT,
  verified_improvement BOOLEAN,
  reward_date TIMESTAMP,
  ump_version STRING
);

CREATE OR REPLACE TABLE CUSTOMER_SEGMENTS (
  customer_segment STRING,
  avg_ltv FLOAT,
  avg_sat FLOAT,
  service_tier STRING
);

-- 2. Simulated Data
INSERT INTO FLEET_TELEMETRY
SELECT UUID_STRING(), 'V'||SEQ4(), 'D'||UNIFORM(1,20,RANDOM()),
       DATEADD('minute', -UNIFORM(1,5000,RANDOM()), CURRENT_TIMESTAMP()),
       34.05 + RANDOM()/100, -118.25 + RANDOM()/100,
       UNIFORM(40,100,RANDOM()), UNIFORM(5,60,RANDOM()),
       UNIFORM(10,80,RANDOM()), 'R'||UNIFORM(1,15,RANDOM()),
       ARRAY_CONSTRUCT('Sunny','Rainy','Foggy','Windy')[UNIFORM(0,3,RANDOM())]::STRING,
       UNIFORM(20,200,RANDOM())
FROM TABLE(GENERATOR(ROWCOUNT=>200));

INSERT INTO AI_PREDICTIONS
SELECT UUID_STRING(), 'D'||UNIFORM(1,20,RANDOM()), 'V'||UNIFORM(1,20,RANDOM()),
       'R'||UNIFORM(1,15,RANDOM()), CURRENT_TIMESTAMP(),
       ROUND(UNIFORM(2,10,RANDOM()),2), ROUND(UNIFORM(1000,5000,RANDOM()),2),
       ROUND(UNIFORM(70,100,RANDOM()),2), ROUND(UNIFORM(0.1,0.9,RANDOM()),2),
       ARRAY_CONSTRUCT('v1.0','v1.1','v2.0')[UNIFORM(0,2,RANDOM())]::STRING,
       ARRAY_CONSTRUCT('Reduce Idle Time','Optimize Route','Leave Early','Adjust Speed')[UNIFORM(0,3,RANDOM())]::STRING,
       UNIFORM(0,1,RANDOM())>0.1
FROM TABLE(GENERATOR(ROWCOUNT=>100));

INSERT INTO REALIZED_OUTCOMES
SELECT UUID_STRING(), p.prediction_id, UNIFORM(7,14,RANDOM()),
       ROUND(p.predicted_roi + UNIFORM(-1,1,RANDOM()),2),
       ROUND(p.predicted_sat + UNIFORM(-3,3,RANDOM()),2),
       ROUND(p.predicted_ltv + UNIFORM(-300,300,RANDOM()),2),
       TRUE, DATEADD('day',10,p.prediction_time)
FROM AI_PREDICTIONS p SAMPLE (80);

INSERT INTO REWARDS_LEDGER
SELECT UUID_STRING(), p.driver_id, p.prediction_id,
       ROUND(p.predicted_roi*0.5,2), TRUE,
       DATEADD('day',10,p.prediction_time), p.ump_version
FROM AI_PREDICTIONS p WHERE p.ump_compliant = TRUE SAMPLE (70);

INSERT INTO CUSTOMER_SEGMENTS VALUES
('Premium',4800,95,'Gold'),
('Business',3200,88,'Silver'),
('Standard',2100,82,'Bronze'),
('Economy',1400,76,'Basic');

-- 3. Views for Dashboard
CREATE OR REPLACE VIEW VW_ROI_ACCURACY AS
SELECT p.driver_id,
       AVG(p.predicted_roi) AS avg_predicted_roi,
       AVG(r.realized_roi) AS avg_realized_roi,
       AVG(ABS(p.predicted_roi - r.realized_roi)) AS avg_error,
       100 - (AVG(ABS(p.predicted_roi - r.realized_roi)) / NULLIF(AVG(r.realized_roi),0) * 100) AS accuracy_percent
FROM AI_PREDICTIONS p JOIN REALIZED_OUTCOMES r ON p.prediction_id = r.prediction_id
GROUP BY p.driver_id;

CREATE OR REPLACE VIEW VW_UMP_COMPLIANCE AS
SELECT ump_version,
       COUNT_IF(ump_compliant) AS compliant_actions,
       COUNT(*) AS total_actions,
       ROUND((COUNT_IF(ump_compliant)/COUNT(*))*100,2) AS compliance_rate
FROM AI_PREDICTIONS GROUP BY ump_version;

CREATE OR REPLACE VIEW VW_REWARD_EFFICIENCY AS
SELECT r.driver_id, SUM(l.token_amount) AS total_tokens,
       AVG(o.realized_roi) AS avg_realized_roi,
       ROUND(SUM(o.realized_roi)/NULLIF(SUM(l.token_amount),0),2) AS roi_per_token
FROM REWARDS_LEDGER l
JOIN REALIZED_OUTCOMES o ON l.prediction_id = o.prediction_id
JOIN AI_PREDICTIONS r ON l.prediction_id = r.prediction_id
GROUP BY r.driver_id;

CREATE OR REPLACE VIEW VW_ESG_IMPACT AS
SELECT t.vehicle_id,
       SUM(t.fuel_used_liters) AS total_fuel,
       SUM(t.idle_time_minutes) AS total_idle_time,
       ROUND(SUM(t.distance_km*0.23),2) AS estimated_co2_kg,
       ROUND(AVG(p.predicted_roi),2) AS avg_roi,
       ROUND(AVG(p.predicted_sat),2) AS avg_sat
FROM FLEET_TELEMETRY t JOIN AI_PREDICTIONS p USING(vehicle_id)
GROUP BY t.vehicle_id;
