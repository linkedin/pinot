SET FOREIGN_KEY_CHECKS = 0;
drop table if EXISTS generic_json_entity;
drop TABLE if EXISTS anomaly_function_index;
drop table if EXISTS metric_index;
DROP TABLE if EXISTS anomaly_merge_config_index;
DROP TABLE if EXISTS email_configuration_index;
DROP TABLE if EXISTS task_index;
DROP TABLE if EXISTS job_index;
drop TABLE if EXISTS raw_anomaly_result_index;
DROP TABLE if EXISTS merged_anomaly_result_index;
DROP table if EXISTS anomaly_feedback_index;
DROP TABLE if EXISTS ingraph_dashboard_config_index;
DROP TABLE if EXISTS ingraph_metric_config_index;
DROP TABLE if EXISTS webapp_config_index;
DROP TABLE if EXISTS override_config_index;
DROP TABLE if EXISTS alert_config_index;
SET FOREIGN_KEY_CHECKS = 1;
