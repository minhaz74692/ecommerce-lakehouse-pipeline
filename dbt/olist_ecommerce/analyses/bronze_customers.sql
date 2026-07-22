SELECT
*
FROM {{ source('bronze', 'customers') }}