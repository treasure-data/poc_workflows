WITH BASE AS (
SELECT 
${globals.canonical_id}, 
recency,  
frequency, 
monetary_value,
NTILE(4) OVER (ORDER BY recency DESC) AS r_quartile,
NTILE(4) OVER (ORDER BY TRY(CAST(frequency AS INTEGER))) AS f_quartile,
CASE 
WHEN monetary_value < 0.05 THEN 1
ELSE NTILE(4) OVER (ORDER BY monetary_value) 
END AS m_quartile
FROM ${input_table}
)
SELECT BASE.*,
'R' || CAST(r_quartile AS VARCHAR) || 'F' || CAST(f_quartile AS VARCHAR) || 'M' || CAST(m_quartile AS VARCHAR) AS rfm_quartile,
ROUND((r_quartile + f_quartile + m_quartile)*1.0 / 3.0, 2) as rfm_score,
CASE
    WHEN r_quartile = 4 AND f_quartile = 4 AND m_quartile = 4 THEN 'Champions'
    WHEN r_quartile >= 3 THEN
        CASE
            WHEN f_quartile >= 3 AND m_quartile >= 3 THEN 'Loyal Customers'
            WHEN f_quartile >= 2 AND m_quartile >= 2 THEN 'Potential Loyalists'
            WHEN f_quartile >= 2 OR m_quartile >= 2 THEN 'Promising'
            ELSE 'New Customers'
        END
    WHEN r_quartile = 2 THEN
        CASE
            WHEN m_quartile >= 3 THEN 'Cannot lose them'
            WHEN m_quartile = 2 AND f_quartile >= 2 THEN 'Need attention'
            ELSE 'Hibernating'
        END
    WHEN r_quartile = 1 THEN
        CASE
            WHEN m_quartile >= 3 OR (m_quartile = 2 AND f_quartile >= 2) THEN 'High Value Sleeping'
            ELSE 'Lost customers'
        END
    ELSE NULL -- Optionally handle unexpected values here
END AS rfm_segment
FROM BASE
