SELECT
    PROPERTY_ID,
    OWNER_TYPE,
    OWNER_NAMES,
    OWNER_ZIP,
    OWNER_CITY,
    OWNER_STATE
FROM {{ ref('stg_property_data') }}
