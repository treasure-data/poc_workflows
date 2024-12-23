SELECT array_join(array_agg(column_filter), ' AND ') as approval_filter
from prob_approval_schema