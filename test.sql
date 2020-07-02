with sfo as (
    select
        *
    from
        (select
            advertising_id,
            country,
            event_date as install_date,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            default.bigquery_first_open
        where
            app_id = 'com.zitga.ninja.stickman.legends'
            and advertising_id is not null
            and platform = 'ANDROID'
            and event_date <= '${end_date}') a
    where
        row_num = 1
        and install_date between '${start_date}' and '${end_date}' ),
sue as (
    select
        distinct
        advertising_id,
        event_date as active_date
    from
        default.bigquery_user_engagement
    where
        app_id = 'com.zitga.ninja.stickman.legends'
        and advertising_id is not null
        and platform = 'ANDROID'
        and event_date between '${start_date}' and date_add('${end_date}', 7) ),
siap as (
    select
        advertising_id,
        event_date as buy_date,
        count(*) as buy_count,
        sum(event_value_in_usd) as revenue
    from
        default.bigquery_in_app_purchase
    where
        app_id = 'com.zitga.ninja.stickman.legends'
        and advertising_id is not null
        and platform = 'ANDROID'
        and event_date between '${start_date}' and date_add('${end_date}', 7)
    group by
        advertising_id,
        event_date ),
sfoe as (
    select
        country,
        install_date,
        count(distinct sfo.advertising_id) as install,
        sum(case when datediff(active_date, install_date) = 1 then 1 else 0 end) as is_r1,
        sum(case when datediff(active_date, install_date) = 2 then 1 else 0 end) as is_r2,
        sum(case when datediff(active_date, install_date) = 3 then 1 else 0 end) as is_r3,
        sum(case when datediff(active_date, install_date) = 4 then 1 else 0 end) as is_r4,
        sum(case when datediff(active_date, install_date) = 5 then 1 else 0 end) as is_r5,
        sum(case when datediff(active_date, install_date) = 6 then 1 else 0 end) as is_r6,
        sum(case when datediff(active_date, install_date) = 7 then 1 else 0 end) as is_r7
    from
        sfo
    left join sue on
        sfo.advertising_id = sue.advertising_id
    group by
        country,
        install_date ),
sfoi as (
    select
        sfo.country,
        sfo.install_date,
        count(distinct iap.advertising_id) as buyer,
        sum(coalesce(buy_count, 0)) as buy_count,
        sum(coalesce(revenue, 0)) as revenue,
        sum(case when datediff(buy_date, install_date) < 1 then revenue else 0 end) as rev1,
        sum(case when datediff(buy_date, install_date) < 3 then revenue else 0 end) as rev3,
        sum(case when datediff(buy_date, install_date) < 7 then revenue else 0 end) as rev7,
        sum(case when datediff(buy_date, install_date) < 1 then buy_count else 0 end) as buy1,
        sum(case when datediff(buy_date, install_date) < 3 then buy_count else 0 end) as buy3,
        sum(case when datediff(buy_date, install_date) < 7 then buy_count else 0 end) as buy7
    from
        sfo
    left join
        iap on sfo.advertising_id = iap.advertising_id
    group by
        sfo.country,
        sfo.install_date),
sl as (
  select
      "SL" as game,
      coalesce(foe.country, "Unknown") as country,
      sfoe.install_date,
      sfoe.install,
      coalesce(buyer, 0) as buyer,
      coalesce(buy_count, 0) as buy_count,
      coalesce(revenue, 0) as revenue,
      coalesce(rev1, 0) as rev1,
      coalesce(rev3, 0) as rev3,
      coalesce(rev7, 0) as rev7,
      coalesce(buy1, 0) as buy1,
      coalesce(buy3, 0) as buy3,
      coalesce(buy7, 0) as buy7,
      is_r1,
      is_r2,
      is_r3,
      is_r4,
      is_r5,
      is_r6,
      is_r7
  from
      sfoe
  left join sfoi on
      sfoe.country = sfoi.country
      and sfoe.install_date = sfoi.install_date)
