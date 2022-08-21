CREATE TABLE IF NOT EXISTS IVIVCHIKYANDEXRU__DWH.s_auth_group_history(
    hk_l_user_group_activity    BIGINT REFERENCES IVIVCHIKYANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from                INT,
    "event"                     VARCHAR(10),
    event_dt                    DATETIME,
    load_dt                     DATETIME,
    load_src                    VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);