{% snapshot snapshot_session_summary %}

{{ config(
    target_schema="snapshot",
    unique_key="sessionId",
    strategy="timestamp",
    updated_at="timestamp",
    invalidate_hard_deletes=True
) }}

SELECT * FROM {{ ref("session_summary") }}

{% endsnapshot %}
