-- ניקוי לקוחות: נרמול מייל/עיר, פירוק תאריכים בשני פורמטים, דה-דופליקציה
with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        cast(customer_id as integer)                         as customer_id,
        trim(full_name)                                      as full_name,
        lower(trim(email))                                   as email,
        case when trim(city) = 'Tel Aviv' then 'תל אביב'
             else trim(city) end                             as city,
        coalesce(
            try_strptime(signup_date, '%Y-%m-%d'),
            try_strptime(signup_date, '%d/%m/%Y')
        )::date                                              as signup_date,
        cast(updated_at as timestamp)                        as updated_at
    from source
)

-- אותו לקוח הופיע כמה פעמים: שומרים את הגרסה העדכנית
select * from cleaned
qualify row_number() over (partition by customer_id order by updated_at desc) = 1
