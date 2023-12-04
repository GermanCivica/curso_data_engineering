-- MACRO PARA COMPROBAR QUE LAS FECHAS DE UNA COLUMNA ESTÁN EN UTC
{% test utc_times(model, column_name) %}

    -- Tabla con 2 columnas, fecha que tenemos en el modelo y otra con el resutlado de la comprobación de si su uso horario es UTC
    -- Excluimos los nulos de nuestro análisis
    with utc_dates as (
        select 
              {{ column_name }}
            , CASE
              WHEN {{ dbt_date.convert_timezone(column_name, "UTC") }} = {{ column_name }} THEN True
              ELSE False
              END AS is_utc_date
        from {{ model }}
        where {{ column_name }} is not null 
    )

    -- Seleccionamos solo los registros que se haya comprobado que no están en UTC (en caso de que existan)
    select * from utc_dates
    where is_utc_date = False

{% endtest %}