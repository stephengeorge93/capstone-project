SELECT
    PROPERTY_ID,
    VALUE_2023,
    LAND_2023,
    IMPROVEMENTS_2023,
    VALUE_2024,
    LAND_2024,
    IMPROVEMENTS_2024,
    TAX_2023,
    LOAD_DATE
FROM {{ ref('stg_property_data') }}
