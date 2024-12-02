select DISTINCT time as time_unixtime, substr(cast(from_unixtime(time) as VARCHAR),1,10) as datetime
  ,(SELECT avg_id FROM ${prefix}avg_min_max WHERE id_type = 'email') as avg_emails 
  ,(SELECT avg_id FROM ${prefix}avg_min_max WHERE id_type = 'phone_number') as avg_phone
  ,(SELECT avg_id FROM ${prefix}avg_min_max WHERE id_type = 'contactid') as contactid
from ${prefix}avg_min_max a