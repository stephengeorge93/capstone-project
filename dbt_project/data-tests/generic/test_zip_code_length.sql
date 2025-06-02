SELECT *
FROM {{ source('rentcast', 'rentcast_properties') }}
WHERE LENGTH(CAST(zip_code AS STRING)) != 5