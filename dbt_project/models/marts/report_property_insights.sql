WITH valuation AS (
    SELECT * FROM {{ ref('fact_property_valuation') }}
),
location AS (
    SELECT * FROM {{ ref('dim_property_location') }}
),
structure AS (
    SELECT * FROM {{ ref('dim_property_structure') }}
),
owner AS (
    SELECT * FROM {{ ref('dim_owner') }}
)

SELECT
    v.PROPERTY_ID,
    l.FORMATTED_ADDRESS,
    l.ADDRESS_LINE_1,
    l.CITY,
    l.STATE,
    l.ZIP_CODE,
    l.COUNTY,
    l.LATITUDE,
    l.LONGITUDE,
    l.SUBDIVISION,
    s.PROPERTY_TYPE,
    s.BEDROOMS,
    s.BATHROOMS,
    s.SQUARE_FOOTAGE,
    s.LOT_SIZE,
    s.YEAR_BUILT,
    s.FLOOR_COUNT,
    s.GARAGE,
    s.GARAGE_SPACES,
    s.GARAGE_TYPE,
    s.HEATING_TYPE,
    s.COOLING_TYPE,
    s.ROOF_TYPE,
    s.UNIT_COUNT,
    s.ROOM_COUNT,
    s.ARCHITECTURE_TYPE,
    v.VALUE_2023,
    v.LAND_2023,
    v.IMPROVEMENTS_2023,
    v.VALUE_2024,
    v.LAND_2024,
    v.IMPROVEMENTS_2024,
    v.TAX_2023,
    ROUND((v.VALUE_2024 - v.VALUE_2023) / NULLIF(v.VALUE_2023, 0), 2) AS value_growth_pct,
    o.OWNER_TYPE,
    o.OWNER_NAMES,
    o.OWNER_ZIP,
    o.OWNER_CITY,
    o.OWNER_STATE,
    v.LOAD_DATE
FROM valuation v
LEFT JOIN location l ON v.PROPERTY_ID = l.PROPERTY_ID
LEFT JOIN structure s ON v.PROPERTY_ID = s.PROPERTY_ID
LEFT JOIN owner o ON v.PROPERTY_ID = o.PROPERTY_ID
