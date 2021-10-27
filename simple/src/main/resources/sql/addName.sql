select
       id,
       uuid() as uuid,
       concat('name_', ID%7) as name,
       concat('description_', ID%9) as description
from is
where ID < ${num:-999999}