with slu as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as install_date,
            country,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            default.bigquery_first_open
        where
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) <= date_add(date(from_unixtime(unix_timestamp())), -4)
            and app_id in ('com.zitga.ninja.stickman.legends', 'com.zitga.ninja.stickman.legends.shadow.wars') ) a
    where
        install_date >= '2020-02-01'
        and row_num = 1 ),
slfa as (
    select
        lower(advertising_id) as advertising_id,
        event_date,
        app_id,
        platform,
        sum(revenue) as ads_revenue
    from
        default.iron_source_ad_revenue_report
    where
        event_date between '2020-02-01' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.zitga.ninja.stickman.legends', 'com.zitga.ninja.stickman.legends.shadow.wars')
    group by
        lower(advertising_id),
        event_date,
        app_id,
        platform ),
slfi as (
    select
        lower(advertising_id) as advertising_id,
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as event_date,
        app_id,
        platform,
        sum(event_value_in_usd) as iap_revenue
    from
        default.bigquery_in_app_purchase
    where
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) between '2020-02-01' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.zitga.ninja.stickman.legends', 'com.zitga.ninja.stickman.legends.shadow.wars')
    group by
        lower(advertising_id),
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))),
        app_id,
        platform ),
sla as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            case when media_source is null then 'Organic' else media_source end as media_source,
            case
                when media_source is null then 'Organic'
                when campaign is null then media_source
                else campaign
            end as campaign,
            row_number() over(partition by advertising_id order by event_time desc) as row_num
        from
            default.appsflyer_report_install_v3
        where
            app_id in ('com.zitga.ninja.stickman.legends', 'com.zitga.ninja.stickman.legends.shadow.wars', 'id1186523572') ) a
    where
        row_num = 1 ),
slti as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Stickman Legends" as Project,
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_rev
    from
        slfi
    left join sla
        on slfi.advertising_id = sla.advertising_id
    inner join slu
        on slfi.advertising_id = slu.advertising_id
    left join default.vietpm_country_code v
        on slu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
slta as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Stickman Legends" as Project,
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_rev
    from
        slfa
    left join sla
        on slfa.advertising_id = sla.advertising_id
    inner join slu
        on slfa.advertising_id = slu.advertising_id
    left join default.vietpm_country_code v
        on slu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
slbi as (
    select
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Stickman Legends" as Project,
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_brev
    from
        slfi
    left join sla
        on slfi.advertising_id = sla.advertising_id
    left join slu
        on slfi.advertising_id = slu.advertising_id
    left join default.vietpm_country_code v
        on slu.country = v.fb_country
    group by
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
slba as (
    select
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Stickman Legends" as Project,
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_brev
    from
        slfa
    left join sla
        on slfa.advertising_id = sla.advertising_id
    left join slu
        on slfa.advertising_id = slu.advertising_id
    left join default.vietpm_country_code v
        on slu.country = v.fb_country
    group by
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
slaf as (
    select
        app_id,
        'Stickman Legends' as Project,
    	case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    	end as Version,
        upper(platform) as platform,
        event_date,
        coalesce(country_code, "N/A") as country_code,
        country,
        media_source,
        campaign,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(total_cost) as total_cost
    from
        default.appsflyer_report_geo_by_date
    where
        event_date >= '2020-02-01'
        and app_id in ('com.zitga.ninja.stickman.legends',
        'com.zitga.ninja.stickman.legends.shadow.wars')
    group by
        app_id,
        case
    		when app_id = 'com.zitga.ninja.stickman.legends' then 'Free'
    		when app_id = 'com.zitga.ninja.stickman.legends.shadow.wars' then 'Paid'
    	end,
        upper(platform),
        event_date,
        coalesce(country_code, "N/A"),
        country,
        media_source,
        campaign),
sl as (
    select
        coalesce(slaf.Project, slti.Project, slta.Project, slba.Project, slbi.Project) as Project,
        coalesce(slaf.Version, slti.Version, slta.Version, slba.Version, slbi.Version) as Version,
        coalesce(slaf.platform, slti.platform, slta.platform, slba.platform, slbi.platform) as platform,
        coalesce(slaf.country_code, slti.country_code, slta.country_code, slba.country_code, slbi.country_code) as country_code,
        coalesce(slaf.country, slti.country, slta.country, slba.country, slbi.country) as country,
        coalesce(slaf.media_source, slti.media_source, slta.media_source, slba.media_source, slbi.media_source) as media_source,
        coalesce(slaf.campaign, slti.campaign, slta.campaign, slba.campaign, slbi.campaign) as campaign,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(installs, 0) as installs,
        coalesce(total_cost, 0) as total_cost,
        coalesce(iap_rev, 0) as iap_rev,
        coalesce(ads_rev, 0) as ads_rev,
        coalesce(iap_brev, 0) as iap_brev,
        coalesce(ads_brev, 0) as ads_brev,
        coalesce(slaf.event_date, slti.event_date, slta.event_date, slba.event_date, slbi.event_date) as event_date,
        coalesce(slaf.app_id, slti.app_id, slta.app_id, slba.app_id, slbi.app_id) as app_id
    from
        slaf
    full join slti on
        slaf.app_id = slti.app_id
        and slaf.platform = slti.platform
        and slaf.event_date = slti.event_date
        and slaf.country_code = slti.country_code
        and slaf.media_source = slti.media_source
        and slaf.campaign = slti.campaign
    full join slta on
        slaf.app_id = slta.app_id
        and slaf.platform = slta.platform
        and slaf.event_date = slta.event_date
        and slaf.country_code = slta.country_code
        and slaf.media_source = slta.media_source
        and slaf.campaign = slta.campaign
    full join slbi on
        slaf.app_id = slbi.app_id
        and slaf.platform = slbi.platform
        and slaf.event_date = slbi.event_date
        and slaf.country_code = slbi.country_code
        and slaf.media_source = slbi.media_source
        and slaf.campaign = slbi.campaign
    full join slba on
        slaf.app_id = slba.app_id
        and slaf.platform = slba.platform
        and slaf.event_date = slba.event_date
        and slaf.country_code = slba.country_code
        and slaf.media_source = slba.media_source
        and slaf.campaign = slba.campaign),
---- TD query
tdu as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as install_date,
            country,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            empire_warriors_td.bigquery_first_open
        where
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) <= date_add(date(from_unixtime(unix_timestamp())), -4)
            and app_id in ('com.zitga.empire.warriors.td', 'com.zitga.empire.warriors.td.tower.defense') ) a
    where
        install_date >= '2020-02-01'
        and row_num = 1 ),
tdfa as (
    select
        lower(advertising_id) as advertising_id,
        event_date,
        app_id,
        platform,
        sum(revenue) as ads_revenue
    from
        default.iron_source_ad_revenue_report
    where
        event_date between '2020-02-01' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.zitga.empire.warriors.td', 'com.zitga.empire.warriors.td.tower.defense')
    group by
        lower(advertising_id),
        event_date,
        app_id,
        platform ),
tdfi as (
    select
        lower(advertising_id) as advertising_id,
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as event_date,
        app_id,
        platform,
        sum(event_value_in_usd) as iap_revenue
    from
        empire_warriors_td.bigquery_in_app_purchase
    where
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) between '2020-02-01' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.zitga.empire.warriors.td', 'com.zitga.empire.warriors.td.tower.defense')
    group by
        lower(advertising_id),
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))),
        app_id,
        platform ),
tda as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            case when media_source is null then 'Organic' else media_source end as media_source,
            case
                when media_source is null then 'Organic'
                when campaign is null then media_source
                else campaign
            end as campaign,
            row_number() over(partition by advertising_id order by event_time desc) as row_num
        from
            default.appsflyer_report_install_v3
        where
            app_id in ('com.zitga.empire.warriors.td', 'com.zitga.empire.warriors.td.tower.defense', 'id1329443393') ) a
    where
        row_num = 1 ),
tdti as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Empire Warriors TD" as Project,
        case
    		when app_id = 'com.zitga.empire.warriors.td' then 'Free'
    		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_rev
    from
        tdfi
    left join tda
        on tdfi.advertising_id = tda.advertising_id
    inner join tdu
        on tdfi.advertising_id = tdu.advertising_id
    left join default.vietpm_country_code v
        on tdu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
    		when app_id = 'com.zitga.empire.warriors.td' then 'Free'
    		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
tdta as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Empire Warriors TD" as Project,
        case
        when app_id = 'com.zitga.empire.warriors.td' then 'Free'
    		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    		else 'Other'
    	end as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_rev
    from
        tdfa
    left join tda
        on tdfa.advertising_id = tda.advertising_id
    inner join tdu
        on tdfa.advertising_id = tdu.advertising_id
    left join default.vietpm_country_code v
        on tdu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        case
        when app_id = 'com.zitga.empire.warriors.td' then 'Free'
    		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    		else 'Other'
    	end,
        media_source,
        campaign ),
        tdbi as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Empire Warriors TD" as Project,
                case
            		when app_id = 'com.zitga.empire.warriors.td' then 'Free'
            		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
            		else 'Other'
            	end as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(iap_revenue) as iap_brev
            from
                tdfi
            left join tda
                on tdfi.advertising_id = tda.advertising_id
            left join tdu
                on tdfi.advertising_id = tdu.advertising_id
            left join default.vietpm_country_code v
                on tdu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                case
            		when app_id = 'com.zitga.empire.warriors.td' then 'Free'
            		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
            		else 'Other'
            	end,
                media_source,
                campaign ),
        tdba as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Empire Warriors TD" as Project,
                case
                when app_id = 'com.zitga.empire.warriors.td' then 'Free'
            		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
            		else 'Other'
            	end as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(ads_revenue) as ads_brev
            from
                tdfa
            left join tda
                on tdfa.advertising_id = tda.advertising_id
            left join tdu
                on tdfa.advertising_id = tdu.advertising_id
            left join default.vietpm_country_code v
                on tdu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                case
                when app_id = 'com.zitga.empire.warriors.td' then 'Free'
            		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
            		else 'Other'
            	end,
                media_source,
                campaign ),
tdaf as (
    select
        app_id,
        'Empire Warriors TD' as Project,
    	case
      when app_id = 'com.zitga.empire.warriors.td' then 'Free'
      when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    	end as Version,
        upper(platform) as platform,
        event_date,
        coalesce(country_code, "N/A") as country_code,
        country,
        media_source,
        campaign,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(total_cost) as total_cost
    from
        default.appsflyer_report_geo_by_date
    where
        event_date >= '2020-02-01'
        and app_id in ('com.zitga.empire.warriors.td', 'com.zitga.empire.warriors.td.tower.defense')
    group by
        app_id,
        case
        when app_id = 'com.zitga.empire.warriors.td' then 'Free'
    		when app_id = 'com.zitga.empire.warriors.td.tower.defense' then 'Paid'
    	end,
        upper(platform),
        event_date,
        coalesce(country_code, "N/A"),
        country,
        media_source,
        campaign),
td as (
    select
        coalesce(tdaf.Project, tdti.Project, tdta.Project, tdba.Project, tdbi.Project) as Project,
        coalesce(tdaf.Version, tdti.Version, tdta.Version, tdba.Version, tdbi.Version) as Version,
        coalesce(tdaf.platform, tdti.platform, tdta.platform, tdba.platform, tdbi.platform) as platform,
        coalesce(tdaf.country_code, tdti.country_code, tdta.country_code, tdba.country_code, tdbi.country_code) as country_code,
        coalesce(tdaf.country, tdti.country, tdta.country, tdba.country, tdbi.country) as country,
        coalesce(tdaf.media_source, tdti.media_source, tdta.media_source, tdba.media_source, tdbi.media_source) as media_source,
        coalesce(tdaf.campaign, tdti.campaign, tdta.campaign, tdba.campaign, tdbi.campaign) as campaign,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(installs, 0) as installs,
        coalesce(total_cost, 0) as total_cost,
        coalesce(iap_rev, 0) as iap_rev,
        coalesce(ads_rev, 0) as ads_rev,
        coalesce(iap_brev, 0) as iap_brev,
        coalesce(ads_brev, 0) as ads_brev,
        coalesce(tdaf.event_date, tdti.event_date, tdta.event_date, tdba.event_date, tdbi.event_date) as event_date,
        coalesce(tdaf.app_id, tdti.app_id, tdta.app_id, tdba.app_id, tdbi.app_id) as app_id
    from
        tdaf
    full join tdti on
        tdaf.app_id = tdti.app_id
        and tdaf.platform = tdti.platform
        and tdaf.event_date = tdti.event_date
        and tdaf.country_code = tdti.country_code
        and tdaf.media_source = tdti.media_source
        and tdaf.campaign = tdti.campaign
    full join tdta on
        tdaf.app_id = tdta.app_id
        and tdaf.platform = tdta.platform
        and tdaf.event_date = tdta.event_date
        and tdaf.country_code = tdta.country_code
        and tdaf.media_source = tdta.media_source
        and tdaf.campaign = tdta.campaign
    full join tdbi on
        tdaf.app_id = tdbi.app_id
        and tdaf.platform = tdbi.platform
        and tdaf.event_date = tdbi.event_date
        and tdaf.country_code = tdbi.country_code
        and tdaf.media_source = tdbi.media_source
        and tdaf.campaign = tdbi.campaign
    full join tdba on
        tdaf.app_id = tdba.app_id
        and tdaf.platform = tdba.platform
        and tdaf.event_date = tdba.event_date
        and tdaf.country_code = tdba.country_code
        and tdaf.media_source = tdba.media_source
        and tdaf.campaign = tdba.campaign),
---- SE query
seu as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as install_date,
            country,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            summoners_era.bigquery_first_open
        where
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) <= date_add(date(from_unixtime(unix_timestamp())), -4)
            and app_id = 'com.fansipan.summoners.era.idle' ) a
    where
        install_date >= '2020-02-11'
        and row_num = 1 ),
sefa as (
    select
        lower(advertising_id) as advertising_id,
        event_date,
        app_id,
        platform,
        sum(revenue) as ads_revenue
    from
        summoners_era.iron_source_ad_revenue_report
    where
        event_date between '2020-02-11' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id = 'com.fansipan.summoners.era.idle'
    group by
        lower(advertising_id),
        event_date,
        app_id,
        platform ),
sefi as (
    select
        lower(advertising_id) as advertising_id,
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as event_date,
        app_id,
        platform,
        sum(event_value_in_usd) as iap_revenue
    from
        summoners_era.bigquery_in_app_purchase
    where
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) between '2020-02-11' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id = 'com.fansipan.summoners.era.idle'
    group by
        lower(advertising_id),
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))),
        app_id,
        platform ),
sea as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            case when media_source is null then 'Organic' else media_source end as media_source,
            case
                when media_source is null then 'Organic'
                when campaign is null then media_source
                else campaign
            end as campaign,
            row_number() over(partition by advertising_id order by event_time desc) as row_num
        from
            summoners_era.appsflyer_report_install_v3
        where
            app_id = 'com.fansipan.summoners.era.idle' ) a
    where
        row_num = 1 ),
seti as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Summoners Era" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_rev
    from
        sefi
    left join sea
        on sefi.advertising_id = sea.advertising_id
    inner join seu
        on sefi.advertising_id = seu.advertising_id
    left join default.vietpm_country_code v
        on seu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
seta as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Summoners Era" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_rev
    from
        sefa
    left join sea
        on sefa.advertising_id = sea.advertising_id
    inner join seu
        on sefa.advertising_id = seu.advertising_id
    left join default.vietpm_country_code v
        on seu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
        sebi as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Summoners Era" as Project,
                'Free' as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(iap_revenue) as iap_brev
            from
                sefi
            left join sea
                on sefi.advertising_id = sea.advertising_id
            left join seu
                on sefi.advertising_id = seu.advertising_id
            left join default.vietpm_country_code v
                on seu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                media_source,
                campaign ),
        seba as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Summoners Era" as Project,
                'Free' as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(ads_revenue) as ads_brev
            from
                sefa
            left join sea
                on sefa.advertising_id = sea.advertising_id
            left join seu
                on sefa.advertising_id = seu.advertising_id
            left join default.vietpm_country_code v
                on seu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                media_source,
                campaign ),
seaf as (
    select
        app_id,
        'Summoners Era' as Project,
    	   'Free' as Version,
        upper(platform) as platform,
        event_date,
        coalesce(country_code, "N/A") as country_code,
        country,
        media_source,
        campaign,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(total_cost) as total_cost
    from
        summoners_era.appsflyer_report_geo_by_date
    where
        event_date >= '2020-02-11'
        and app_id = 'com.fansipan.summoners.era.idle'
    group by
        app_id,
        upper(platform),
        event_date,
        coalesce(country_code, "N/A"),
        country,
        media_source,
        campaign),
se as (
    select
        coalesce(seaf.Project, seti.Project, seta.Project, seba.Project, sebi.Project) as Project,
        coalesce(seaf.Version, seti.Version, seta.Version, seba.Version, sebi.Version) as Version,
        coalesce(seaf.platform, seti.platform, seta.platform, seba.platform, sebi.platform) as platform,
        coalesce(seaf.country_code, seti.country_code, seta.country_code, seba.country_code, sebi.country_code) as country_code,
        coalesce(seaf.country, seti.country, seta.country, seba.country, sebi.country) as country,
        coalesce(seaf.media_source, seti.media_source, seta.media_source, seba.media_source, sebi.media_source) as media_source,
        coalesce(seaf.campaign, seti.campaign, seta.campaign, seba.campaign, sebi.campaign) as campaign,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(installs, 0) as installs,
        coalesce(total_cost, 0) as total_cost,
        coalesce(iap_rev, 0) as iap_rev,
        coalesce(ads_rev, 0) as ads_rev,
        coalesce(iap_brev, 0) as iap_brev,
        coalesce(ads_brev, 0) as ads_brev,
        coalesce(seaf.event_date, seti.event_date, seta.event_date, seba.event_date, sebi.event_date) as event_date,
        coalesce(seaf.app_id, seti.app_id, seta.app_id, seba.app_id, sebi.app_id) as app_id
    from
        seaf
    full join seti on
        seaf.app_id = seti.app_id
        and seaf.platform = seti.platform
        and seaf.event_date = seti.event_date
        and seaf.country_code = seti.country_code
        and seaf.media_source = seti.media_source
        and seaf.campaign = seti.campaign
    full join seta on
        seaf.app_id = seta.app_id
        and seaf.platform = seta.platform
        and seaf.event_date = seta.event_date
        and seaf.country_code = seta.country_code
        and seaf.media_source = seta.media_source
        and seaf.campaign = seta.campaign
    full join sebi on
        seaf.app_id = sebi.app_id
        and seaf.platform = sebi.platform
        and seaf.event_date = sebi.event_date
        and seaf.country_code = sebi.country_code
        and seaf.media_source = sebi.media_source
        and seaf.campaign = sebi.campaign
    full join seba on
        seaf.app_id = seba.app_id
        and seaf.platform = seba.platform
        and seaf.event_date = seba.event_date
        and seaf.country_code = seba.country_code
        and seaf.media_source = seba.media_source
        and seaf.campaign = seba.campaign),
---- SK query
sku as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as install_date,
            country,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            shadow_knight.bigquery_first_open
        where
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) <= date_add(date(from_unixtime(unix_timestamp())), -4)
            and app_id in ('com.fansipan.stickman.fight.shadow.knights', 'com.fansipan.stickman.fight.shadow.knight') ) a
    where
        install_date >= '2020-03-17'
        and row_num = 1 ),
skfa as (
    select
        lower(advertising_id) as advertising_id,
        event_date,
        app_id,
        platform,
        sum(revenue) as ads_revenue
    from
        shadow_knight.iron_source_ad_revenue_report
    where
        event_date between '2020-03-17' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.fansipan.stickman.fight.shadow.knights', 'com.fansipan.stickman.fight.shadow.knight')
    group by
        lower(advertising_id),
        event_date,
        app_id,
        platform ),
skfi as (
    select
        lower(advertising_id) as advertising_id,
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as event_date,
        app_id,
        platform,
        sum(event_value_in_usd) as iap_revenue
    from
        shadow_knight.bigquery_in_app_purchase
    where
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) between '2020-03-17' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id in ('com.fansipan.stickman.fight.shadow.knights', 'com.fansipan.stickman.fight.shadow.knight')
    group by
        lower(advertising_id),
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))),
        app_id,
        platform ),
ska as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            case when media_source is null then 'Organic' else media_source end as media_source,
            case
                when media_source is null then 'Organic'
                when campaign is null then media_source
                else campaign
            end as campaign,
            row_number() over(partition by advertising_id order by event_time desc) as row_num
        from
            shadow_knight.appsflyer_report_install_v3
        where
            app_id in ('com.fansipan.stickman.fight.shadow.knights', 'com.fansipan.stickman.fight.shadow.knight') ) a
    where
        row_num = 1 ),
skti as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Shadow Knights" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_rev
    from
        skfi
    left join ska
        on skfi.advertising_id = ska.advertising_id
    inner join sku
        on skfi.advertising_id = sku.advertising_id
    left join default.vietpm_country_code v
        on sku.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
skta as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Shadow Knights" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_rev
    from
        skfa
    left join ska
        on skfa.advertising_id = ska.advertising_id
    inner join sku
        on skfa.advertising_id = sku.advertising_id
    left join default.vietpm_country_code v
        on sku.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
skbi as (
    select
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Shadow Knights" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_brev
    from
        skfi
    left join ska
        on skfi.advertising_id = ska.advertising_id
    left join sku
        on skfi.advertising_id = sku.advertising_id
    left join default.vietpm_country_code v
        on sku.country = v.fb_country
    group by
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
skba as (
    select
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Shadow Knights" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_brev
    from
        skfa
    left join ska
        on skfa.advertising_id = ska.advertising_id
    left join sku
        on skfa.advertising_id = sku.advertising_id
    left join default.vietpm_country_code v
        on sku.country = v.fb_country
    group by
        event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
skaf as (
    select
        app_id,
        'Shadow Knights' as Project,
    	   'Free' as Version,
        upper(platform) as platform,
        event_date,
        coalesce(country_code, "N/A") as country_code,
        country,
        media_source,
        campaign,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(total_cost) as total_cost
    from
        shadow_knight.appsflyer_report_geo_by_date
    where
        event_date >= '2020-03-17'
        and app_id in ('com.fansipan.stickman.fight.shadow.knights', 'com.fansipan.stickman.fight.shadow.knight')
    group by
        app_id,
        upper(platform),
        event_date,
        coalesce(country_code, "N/A"),
        country,
        media_source,
        campaign),
sk as (
    select
        coalesce(skaf.Project, skti.Project, skta.Project, skba.Project, skbi.Project) as Project,
        coalesce(skaf.Version, skti.Version, skta.Version, skba.Version, skbi.Version) as Version,
        coalesce(skaf.platform, skti.platform, skta.platform, skba.platform, skbi.platform) as platform,
        coalesce(skaf.country_code, skti.country_code, skta.country_code, skba.country_code, skbi.country_code) as country_code,
        coalesce(skaf.country, skti.country, skta.country, skba.country, skbi.country) as country,
        coalesce(skaf.media_source, skti.media_source, skta.media_source, skba.media_source, skbi.media_source) as media_source,
        coalesce(skaf.campaign, skti.campaign, skta.campaign, skba.campaign, skbi.campaign) as campaign,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(installs, 0) as installs,
        coalesce(total_cost, 0) as total_cost,
        coalesce(iap_rev, 0) as iap_rev,
        coalesce(ads_rev, 0) as ads_rev,
        coalesce(iap_brev, 0) as iap_brev,
        coalesce(ads_brev, 0) as ads_brev,
        coalesce(skaf.event_date, skti.event_date, skta.event_date, skba.event_date, skbi.event_date) as event_date,
        coalesce(skaf.app_id, skti.app_id, skta.app_id, skba.app_id, skbi.app_id) as app_id

    from
        skaf
    full join skti on
        skaf.app_id = skti.app_id
        and skaf.platform = skti.platform
        and skaf.event_date = skti.event_date
        and skaf.country_code = skti.country_code
        and skaf.media_source = skti.media_source
        and skaf.campaign = skti.campaign
    full join skta on
        skaf.app_id = skta.app_id
        and skaf.platform = skta.platform
        and skaf.event_date = skta.event_date
        and skaf.country_code = skta.country_code
        and skaf.media_source = skta.media_source
        and skaf.campaign = skta.campaign
    full join skbi on
        skaf.app_id = skbi.app_id
        and skaf.platform = skbi.platform
        and skaf.event_date = skbi.event_date
        and skaf.country_code = skbi.country_code
        and skaf.media_source = skbi.media_source
        and skaf.campaign = skbi.campaign
    full join skba on
        skaf.app_id = skba.app_id
        and skaf.platform = skba.platform
        and skaf.event_date = skba.event_date
        and skaf.country_code = skba.country_code
        and skaf.media_source = skba.media_source
        and skaf.campaign = skba.campaign),
---- cf query
cfu as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as install_date,
            country,
            row_number() over(partition by advertising_id order by event_timestamp) as row_num
        from
            cyber_fighters.bigquery_first_open
        where
            date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) <= date_add(date(from_unixtime(unix_timestamp())), -4)
            and app_id = 'com.epiczone.cyberfighters.stickman.shadow' ) a
    where
        install_date >= '2020-03-07'
        and row_num = 1 ),
cffa as (
    select
        lower(advertising_id) as advertising_id,
        event_date,
        app_id,
        platform,
        sum(revenue) as ads_revenue
    from
        cyber_fighters.iron_source_ad_revenue_report
    where
        event_date between '2020-03-07' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id = 'com.epiczone.cyberfighters.stickman.shadow'
    group by
        lower(advertising_id),
        event_date,
        app_id,
        platform ),
cffi as (
    select
        lower(advertising_id) as advertising_id,
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) as event_date,
        app_id,
        platform,
        sum(event_value_in_usd) as iap_revenue
    from
        cyber_fighters.bigquery_in_app_purchase
    where
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))) between '2020-03-07' and date_add(date(from_unixtime(unix_timestamp())), -4)
        and app_id = 'com.epiczone.cyberfighters.stickman.shadow'
    group by
        lower(advertising_id),
        date(from_unixtime(cast(event_timestamp / 1000000 as bigint))),
        app_id,
        platform ),
cfa as (
    select
        *
    from
        (select
            lower(advertising_id) as advertising_id,
            case when media_source is null then 'Organic' else media_source end as media_source,
            case
                when media_source is null then 'Organic'
                when campaign is null then media_source
                else campaign
            end as campaign,
            row_number() over(partition by advertising_id order by event_time desc) as row_num
        from
            cyber_fighters.appsflyer_report_install_v3
        where
            app_id = 'com.epiczone.cyberfighters.stickman.shadow' ) a
    where
        row_num = 1 ),
cfti as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Cyber Fighters" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(iap_revenue) as iap_rev
    from
        cffi
    left join cfa
        on cffi.advertising_id = cfa.advertising_id
    inner join cfu
        on cffi.advertising_id = cfu.advertising_id
    left join default.vietpm_country_code v
        on cfu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
cfta as (
    select
        install_date as event_date,
        app_id,
        platform,
        coalesce(country_code, "N/A") as country_code,
        coalesce(v.country, "N/A") as country,
        "Cyber Fighters" as Project,
        'Free' as Version,
        case when media_source is null then 'Unknown' else media_source end as media_source,
        case when campaign is null then 'Unknown' else campaign end as campaign,
        sum(ads_revenue) as ads_rev
    from
        cffa
    left join cfa
        on cffa.advertising_id = cfa.advertising_id
    inner join cfu
        on cffa.advertising_id = cfu.advertising_id
    left join default.vietpm_country_code v
        on cfu.country = v.fb_country
    group by
        install_date,
        app_id,
        platform,
        coalesce(country_code, "N/A"),
        coalesce(v.country, "N/A"),
        media_source,
        campaign ),
        cfbi as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Cyber Fighters" as Project,
                'Free' as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(iap_revenue) as iap_brev
            from
                cffi
            left join cfa
                on cffi.advertising_id = cfa.advertising_id
            left join cfu
                on cffi.advertising_id = cfu.advertising_id
            left join default.vietpm_country_code v
                on cfu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                media_source,
                campaign ),
        cfba as (
            select
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A") as country_code,
                coalesce(v.country, "N/A") as country,
                "Cyber Fighters" as Project,
                'Free' as Version,
                case when media_source is null then 'Unknown' else media_source end as media_source,
                case when campaign is null then 'Unknown' else campaign end as campaign,
                sum(ads_revenue) as ads_brev
            from
                cffa
            left join cfa
                on cffa.advertising_id = cfa.advertising_id
            left join cfu
                on cffa.advertising_id = cfu.advertising_id
            left join default.vietpm_country_code v
                on cfu.country = v.fb_country
            group by
                event_date,
                app_id,
                platform,
                coalesce(country_code, "N/A"),
                coalesce(v.country, "N/A"),
                media_source,
                campaign ),
cfaf as (
    select
        app_id,
        'Cyber Fighters' as Project,
    	   'Free' as Version,
        upper(platform) as platform,
        event_date,
        coalesce(country_code, "N/A") as country_code,
        country,
        media_source,
        campaign,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(total_cost) as total_cost
    from
        cyber_fighters.appsflyer_report_geo_by_date
    where
        event_date >= '2020-03-07'
        and app_id = 'com.epiczone.cyberfighters.stickman.shadow'
    group by
        app_id,
        upper(platform),
        event_date,
        coalesce(country_code, "N/A"),
        country,
        media_source,
        campaign),
cf as (
    select
        coalesce(cfaf.Project, cfti.Project, cfta.Project, cfba.Project, cfbi.Project) as Project,
        coalesce(cfaf.Version, cfti.Version, cfta.Version, cfba.Version, cfbi.Version) as Version,
        coalesce(cfaf.platform, cfti.platform, cfta.platform, cfba.platform, cfbi.platform) as platform,
        coalesce(cfaf.country_code, cfti.country_code, cfta.country_code, cfba.country_code, cfbi.country_code) as country_code,
        coalesce(cfaf.country, cfti.country, cfta.country, cfba.country, cfbi.country) as country,
        coalesce(cfaf.media_source, cfti.media_source, cfta.media_source, cfba.media_source, cfbi.media_source) as media_source,
        coalesce(cfaf.campaign, cfti.campaign, cfta.campaign, cfba.campaign, cfbi.campaign) as campaign,
        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(installs, 0) as installs,
        coalesce(total_cost, 0) as total_cost,
        coalesce(iap_rev, 0) as iap_rev,
        coalesce(ads_rev, 0) as ads_rev,
        coalesce(iap_brev, 0) as iap_brev,
        coalesce(ads_brev, 0) as ads_brev,
        coalesce(cfaf.event_date, cfti.event_date, cfta.event_date, cfba.event_date, cfbi.event_date) as event_date,
        coalesce(cfaf.app_id, cfti.app_id, cfta.app_id, cfba.app_id, cfbi.app_id) as app_id
    from
        cfaf
    full join cfti on
        cfaf.app_id = cfti.app_id
        and cfaf.platform = cfti.platform
        and cfaf.event_date = cfti.event_date
        and cfaf.country_code = cfti.country_code
        and cfaf.media_source = cfti.media_source
        and cfaf.campaign = cfti.campaign
    full join cfta on
        cfaf.app_id = cfta.app_id
        and cfaf.platform = cfta.platform
        and cfaf.event_date = cfta.event_date
        and cfaf.country_code = cfta.country_code
        and cfaf.media_source = cfta.media_source
        and cfaf.campaign = cfta.campaign
    full join cfbi on
        cfaf.app_id = cfbi.app_id
        and cfaf.platform = cfbi.platform
        and cfaf.event_date = cfbi.event_date
        and cfaf.country_code = cfbi.country_code
        and cfaf.media_source = cfbi.media_source
        and cfaf.campaign = cfbi.campaign
    full join cfba on
        cfaf.app_id = cfba.app_id
        and cfaf.platform = cfba.platform
        and cfaf.event_date = cfba.event_date
        and cfaf.country_code = cfba.country_code
        and cfaf.media_source = cfba.media_source
        and cfaf.campaign = cfba.campaign)
select * from (
(select * from sl)
union all
(select * from td)
union all
(select * from se)
union all
(select * from sk)
union all
(select * from cf ) ) a
