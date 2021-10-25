select
       ID,
       concat('name_', ID%7) as name,
       concat('description_', ID%9) as description
from is
where ID < ${num}