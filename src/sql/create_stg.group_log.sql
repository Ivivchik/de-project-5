CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__STAGING.group_log(
group_id         INT REFERENCES IVIVCHIKYANDEXRU__STAGING.groups(id),
user_id          INT REFERENCES IVIVCHIKYANDEXRU__STAGING.users(id),
user_id_from     INT REFERENCES IVIVCHIKYANDEXRU__STAGING.users(id),
"event"          VARCHAR(10),
"datetime"  TIMESTAMP
)
ORDER BY group_id, user_id
segmented by hash(group_id) ALL NODES
PARTITION BY "datetime"::DATE
GROUP BY CALENDAR_HIERARCHY_DAY("datetime"::DATE, 3, 2);