with fo as (
  (select
    *
  from
  (select
    advertising_id,
    datetime(timestamp_micros(min(event_timestamp))) as first_fo,
    datetime(timestamp_micros(max(event_timestamp))) as last_fo
  from
    `api-7752608683054605616-979898.flat_data.first_open_*`
  where
    event_date <= '20200425'
    and app_id = 'com.fansipan.stickman.fight.shadow.knights'
    and advertising_id is not null
  group by
    advertising_id)
  where date(first_fo) between '2020-04-25' and '2020-05-01') ),
tp as (
  select
    advertising_id,
    datetime(timestamp_micros(max(event_timestamp))) as last_tp
  from
    `api-7752608683054605616-979898.flat_data.tab_to_play_*`
  where
    event_date between '20200425' and '20200501'
    and app_id = 'com.fansipan.stickman.fight.shadow.knights'
    and advertising_id is not null
  group by
    advertising_id ),
ue as (
  select
    advertising_id,
    datetime(timestamp_micros(max(event_timestamp))) as last_active
  from
    `api-7752608683054605616-979898.flat_data.user_engagement_*`
  where
    event_date between '20200425' and '20200503'
    and app_id = 'com.fansipan.stickman.fight.shadow.knights'
    and advertising_id is not null
  group by
    advertising_id ),
ar as (
  select
    advertising_id,
    datetime(timestamp_micros(max(event_timestamp))) as last_remove
  from
    `api-7752608683054605616-979898.flat_data.app_remove_*`
  where
    event_date between '20200425' and '20200501'
    and app_id = 'com.fansipan.stickman.fight.shadow.knights'
    and advertising_id is not null
  group by
    advertising_id )
select
  fo.advertising_id,
  replace(cast(first_fo as string), "T", " ") as first_fo,
  replace(cast(last_active as string), "T", " ") as last_active,
  replace(cast(last_fo as string), "T", " ") as last_fo,
  replace(cast(last_remove as string), "T", " ") as last_remove,
  replace(cast(last_tp as string), "T", " ") as last_tp
from
  fo
left join
  tp on fo.advertising_id = tp.advertising_id
left join
  ue on fo.advertising_id = ue.advertising_id
left join ar
  ar on fo.advertising_id = ar.advertising_id;
