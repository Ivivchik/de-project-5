CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.h_groups(
    hk_group_id      BIGINT PRIMARY KEY,
    group_id         INT,
    registration_dt  DATETIME,
    load_dt          DATETIME,
    load_src         VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);