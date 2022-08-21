CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__STAGING.users(
id               INT PRIMARY KEY,
chat_name        VARCHAR(200),
registration_dt  TIMESTAMP,
country          VARCHAR(200),
age              INT
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES;