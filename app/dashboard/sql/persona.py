PERSONA_SQL = """
with email_full as (
    select distinct
        a.profile_id,
        a.email,
        'user' as status
    from
        marketing_emailsubscriber a
),
actions as (
    select
        du.profile_id,
        du.created_on,
        null bounty_id,
        du.action
    from
        dashboard_useraction du

    union all

    select
        da.profile_id,
        da.created created_on,
        da.bounty_id,
        da.activity_type as action
    from
        dashboard_activity da
    order by
        profile_id,
        created_on
),
bounty_hunters_int as (
    select distinct
        du.created_on,
        du.profile_id,
        row_number() over (partition by du.profile_id order by du.created_on desc) rank  -- last instance of contributor action
    from
        actions du
    where
        du.action in ('worker_applied', 'start_work', 'work_submitted', 'stop_work', 'work_done')
    group by
        du.profile_id,
        du.created_on
    order by
        du.profile_id
),
not_active_bounty_hunters_action as (
    select
        a.profile_id,
        'not active bounty hunter' as status
    from
        bounty_hunters_int a
    where
        a.rank = 1
        and
            a.created_on <= date(now()) - interval '3 months'
),
active_bounty_hunters_action as (
    select
        a.profile_id,
        'active bounty hunter' as status
    from
        bounty_hunters_int a
    where
        a.rank = 1
        and
            a.created_on > date(now()) - interval '3 months'
),
active_bounty_hunters_int as (
    select distinct
        date(dbf.created_on) created_on,
        dbf.profile_id,
        dbf.fulfiller_email,
        dbf.fulfiller_github_username,
        row_number() over (partition by dbf.profile_id order by date(dbf.created_on) desc) rank  -- last instance of completed work
    from
        dashboard_bountyfulfillment dbf
    group by
        date(dbf.created_on),
        dbf.profile_id,
        dbf.fulfiller_email,
        dbf.fulfiller_github_username
    order by
        dbf.profile_id
),
active_bounty_hunters_int_2 as (
    select
        *,
        date(now()) - a.created_on days_since_last_completion
    from
        active_bounty_hunters_int a
    where
        a.rank = 1
),
active_bounty_hunters_completion as (
    select distinct
        a.profile_id,
        'active bounty hunter' as status
    from
        active_bounty_hunters_int_2 a
    where
        a.days_since_last_completion < 90
        and
            a.fulfiller_github_username not in (select a.username from auth_user a where a.is_staff = True and a.email is not null and a.email != '' and a.username is not null)
),
not_active_bounty_hunters_completion as (
    select distinct
        a.profile_id,
        'not active bounty hunter' as status
    from
        active_bounty_hunters_int_2 a
    where
        a.days_since_last_completion >= 90
        and
            a.fulfiller_github_username not in (select a.username from auth_user a where a.is_staff = True and a.email is not null and a.email != '' and a.username is not null)
),
funders_int as (
    select
        a.profile_id,
        db.bounty_owner_github_username,
        db.created_on,
        row_number() over (partition by db.bounty_owner_github_username order by db.created_on desc) rank_desc  -- last funded issue
    from
        dashboard_bounty db
    left join (
        select dp.handle, dp.id profile_id from dashboard_profile dp
    ) a
    on
        db.bounty_owner_github_username = a.handle
    where
        db.network = 'mainnet'
        and
            db.current_bounty = True
        and
            db.idx_status in ('open' ,'started', 'submitted', 'done', 'expired', 'cancelled')
        and
            db.bounty_owner_github_username != ''
        and
            db.bounty_owner_github_username not in (select au.username from auth_user au where au.is_staff is True and au.username not in ('consensys', 'superuser') and au.email != '')
),
not_active_funders_completion as (
    select distinct
        f.profile_id,
        'not active funder' as status
    from
        funders_int f
    where
        f.rank_desc = 1
        and
            f.created_on <= date(now()) - interval '3 months'
),
active_funders_completion as (
    select distinct
        f.profile_id,
        'active funder' as status
    from
        funders_int f
    where
        f.rank_desc = 1
        and
            f.created_on > date(now()) - interval '3 months'
),
funders_int_action as (
    select distinct
        du.created_on,
        du.profile_id,
        row_number() over (partition by du.profile_id order by du.created_on desc) rank  -- last instance of funder action
    from
        actions du
    where
        du.action in ('new_bounty', 'worker_approved', 'increase_payout', 'increased_bounty', 'extend_expiration', 'bounty_removed_by_funder')
    group by
        du.profile_id,
        du.created_on
    order by
        du.profile_id
),
not_active_funders_action as (
    select
        a.profile_id,
        'not active funder' as status
    from
        funders_int_action a
    where
        a.rank = 1
        and
            a.created_on <= date(now()) - interval '3 months'
),
active_funders_action as (
    select
        a.profile_id,
        'active funder' as status
    from
        funders_int_action a
    where
        a.rank = 1
        and
            a.created_on > date(now()) - interval '3 months'
),
user_profile as (
    select distinct
        b.id profile_id WHERE profile_id = $1
    from
        dashboard_profile b
),
final as (
    select distinct
        a.profile_id,
        c.email,
        -- b.status::text status_bh_a,
        -- a.status::text status_bh_na,
        -- f.status::text status_f_a,
        -- g.status::text status_f_na,
        coalesce(b.status::text, a.status::text) status_bounty_hunter,
        coalesce(f.status::text, g.status::text) status_funder,
        case when a.status::text is null and b.status::text is null and f.status::text is null and g.status::text is null then c.status::text else null end status_user
        -- array[coalesce(b.status::text, a.status::text), coalesce(f.status::text, g.status::text), c.status::text]
    from
        email_full c
    inner join
        user_profile d
    on
        c.profile_id = d.profile_id
    left join (
        select profile_id, status::text from not_active_bounty_hunters_action
        union
        select profile_id, status::text from not_active_bounty_hunters_completion
    ) a
    on
        c.profile_id = a.profile_id
    left join (
        select profile_id, status::text from active_bounty_hunters_action
        union
        select profile_id, status::text from active_bounty_hunters_completion
    ) b
    on
        c.profile_id = b.profile_id
    left join (
        select profile_id, status::text from active_funders_completion
        union
        select profile_id, status::text from active_funders_action
    ) f
    on
        c.profile_id = f.profile_id
    left join (
        select profile_id, status::text from not_active_funders_completion
        union
        select profile_id, status::text from not_active_funders_action
    ) g
    on
        c.profile_id = g.profile_id
    where
        c.email is not null
    order by
        c.email
)
select * from final;
""".strip()
