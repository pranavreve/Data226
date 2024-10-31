{{ config(materialized='table') }}

with u as (
    select * from {{ ref("user_session_channel") }}
),
st as (
    select * from {{ ref("session_timestamp") }}
)

select
    u.userId,
    u.sessionId,
    u.channel,
    st.timestamp  -- Use "timestamp" here to match the alias in session_timestamp.sql
from u
join st on u.sessionId = st.sessionId
