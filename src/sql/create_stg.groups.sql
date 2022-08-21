CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__STAGING.groups(
id               INT PRIMARY KEY,
admin_id         INT REFERENCES IVIVCHIKYANDEXRU__STAGING.users(id),
group_name       VARCHAR(100),
registration_dt  TIMESTAMP,
is_private       BOOLEAN
)
ORDER BY id, admin_id
segmented by hash(id) ALL NODES
PARTITION BY registration_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(registration_dt::DATE, 3, 2);