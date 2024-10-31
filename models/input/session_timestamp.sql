with session_timestamp as (
    select
        SESSIONID as sessionId,  -- Alias SESSIONID to sessionId
        TS as timestamp  -- Alias TS to timestamp or any other desired name
    from {{ source('raw_data', 'session_timestamp') }}
)
select * from session_timestamp
