-- Create a table to store the data
drop table if exists customers ;

-- Insert sample data into the table
create table customers as

with tmp_customers (email, phone, name, ltv, last_year_transaction_count, next_best_category) as (
VALUES
    ('vishalpatel2890@gmail.com', '860-983-3620', 'Vishal Patel', '$2028', 5, 'Hoodies'),
    ('mike.wolfe@treasure-data.com', '123-456-7890', 'Mike Wolfe', '$4082', 12, 'Joggers'),
    ('vishal.patel+lulu@treasure-data.com', '860-983-3620', 'Vishal Patel', '$3299', 10, 'Hoodies' ),
    ('vishal.patel+lululemon@treasure-data.com', '860-983-3620', 'Vishal Patel', '$3299', 10, 'Hoodies' ),
    ('vishal.patel+demo@treasure-data.com', '203-284-0212', 'Vishal Patel', '$3299', 10, 'Hoodies' )
)

select *, lower(to_hex(SHA256(CAST(to_utf8(email) as VARBINARY)))) as hashed_email
from tmp_customers;
