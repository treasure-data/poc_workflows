

SELECT 
    permutive_id,
    MAX(CASE WHEN tag = 'xid' AND rn = 1 THEN alias END) AS xid,
    MAX(CASE WHEN tag = 'amg' AND rn = 1 THEN alias END) AS amg,
    MAX(CASE WHEN tag = 'amplitudeId' AND rn = 1 THEN alias END) AS amplitudeId,
    MAX(CASE WHEN tag = 'appnexus' AND rn = 1 THEN alias END) AS appnexus,
    MAX(CASE WHEN tag = 'amp' AND rn = 1 THEN alias END) AS amp,
    MAX(CASE WHEN tag = 'ddp' AND rn = 1 THEN alias END) AS ddp,
    MAX(CASE WHEN tag = 'CN_token_id' AND rn = 1 THEN alias END) AS CN_token_id,
    MAX(CASE WHEN tag = 'email_sha256' AND rn = 1 THEN alias END) AS email_sha256,
    MAX(CASE WHEN tag = 'fbclid' AND rn = 1 THEN alias END) AS fbclid,
    MAX(CASE WHEN tag = 'fbp' AND rn = 1 THEN alias END) AS fbp,
    MAX(CASE WHEN tag = 'ga' AND rn = 1 THEN alias END) AS ga,
    MAX(CASE WHEN tag = 'geniusID' AND rn = 1 THEN alias END) AS geniusID,
    MAX(CASE WHEN tag = 'gclid' AND rn = 1 THEN alias END) AS gclid,
    MAX(CASE WHEN tag = 'internal' AND rn = 1 THEN alias END) AS internal,
    MAX(CASE WHEN tag = 'line' AND rn = 1 THEN alias END) AS line,
    MAX(CASE WHEN tag = 'puid' AND rn = 1 THEN alias END) AS puid,
    MAX(CASE WHEN tag = 'pxid' AND rn = 1 THEN alias END) AS pxid,
    MAX(CASE WHEN tag = 'rampId' AND rn = 1 THEN alias END) AS rampId,
    MAX(CASE WHEN tag = 'sailthru_visitor' AND rn = 1 THEN alias END) AS sailthru_visitor,
    MAX(CASE WHEN tag = 'sp_domain_user_id' AND rn = 1 THEN alias END) AS sp_domain_user_id,
    MAX(CASE WHEN tag = 'td_unknown_id' AND rn = 1 THEN alias END) AS td_unknown_id,
    MAX(CASE WHEN tag = 'ttp' AND rn = 1 THEN alias END) AS ttp,
    MAX(CASE WHEN tag = 'uID' AND rn = 1 THEN alias END) AS uID,
    MAX(CASE WHEN tag = '_td_ssc_id' AND rn = 1 THEN alias END) AS td_ssc_id,
    MAX(COALESCE(TD_TIME_PARSE(dt), TD_TIME_PARSE(CAST(CURRENT_TIMESTAMP as STRING)))) as inc_unix 
FROM 
    (SELECT 
        permutive_id,
        tag,
        alias,
        dt,
        ROW_NUMBER() OVER (PARTITION BY permutive_id, tag ORDER BY dt DESC) AS rn
    FROM 
        permutive) data 

WHERE rn = 1
GROUP BY 
    permutive_id
