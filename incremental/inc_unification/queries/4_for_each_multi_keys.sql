with config as (select
    filter(
      TRANSFORM(
        split(
              JSON_EXTRACT_SCALAR(
                  json_parse('${list}')
                  ,'$.key')
              , ',')
      , x -> trim(x))
    , x ->  NULLIF(trim(x), '') is not null) as cols_array
)

select
  col,
  ARRAY_join(TRANSFORM(cols_array, x -> 'b.' || trim(x)), ', ') cols_str
  -- ,'a.' || col || ' in (' || ARRAY_join(TRANSFORM(cols_array, x -> 'b.' || trim(x)), ', ') || ')' as where_clause
from config
cross join UNNEST(cols_array) as t (col)
