CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.l_user_group_activity(
    hk_l_user_group_activity    BIGINT PRIMARY KEY,
    hk_user_id                  INT REFERENCES IVIVCHIKYANDEXRU__DWH.h_users (hk_user_id),
    hk_group_id                 INT REFERENCES IVIVCHIKYANDEXRU__DWH.h_groups (hk_group_id),
    load_dt                     DATETIME,
    load_src                    VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);