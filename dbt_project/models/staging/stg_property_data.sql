SELECT *
FROM {{ source('rentcast', 'rentcast_properties') }}
WHERE property_id IS NOT NULL