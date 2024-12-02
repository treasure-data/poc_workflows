SELECT ${session_id} as session_id,
TD_TIME_STRING(${session_unixtime}, 's!') as session_time,
CAST(current_timestamp AS VARCHAR) AS end_time,
date_diff('minute', from_unixtime(${session_unixtime}),  current_timestamp) as run_duration