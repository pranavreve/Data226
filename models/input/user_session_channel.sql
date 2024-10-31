with user_session as (
    select
        USERID as userId,        -- Alias USERID to userId
        SESSIONID as sessionId,  -- Alias SESSIONID to sessionId
        CHANNEL as channel       -- Alias CHANNEL to channel
    from {{ source('raw_data', 'user_session_channel') }}
)
select * from user_session
