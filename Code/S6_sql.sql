CREATE EXTENSION IF NOT EXISTS postgis;

-- create sensor database
DROP TABLE IF EXISTS sensors_3310;
CREATE TABLE sensors_3310 AS
SELECT DISTINCT ON (sensor_id)            -- keeps the first row for each sensor_id
       sensor_id,
       ST_Transform(geom, 3310) AS geom_3310
FROM   "PurpleAir_after_Calibration"
WHERE  geom IS NOT NULL;

ALTER TABLE sensors_3310
  ADD PRIMARY KEY (sensor_id);

CREATE INDEX sensors_3310_gix
  ON sensors_3310 USING gist (geom_3310);

-- create landuse database
DROP TABLE IF EXISTS landuse_3310;
CREATE TABLE landuse_3310 AS
SELECT id,
       landuse,                            -- <— category column
       ST_Transform(geom, 3310) AS geom_3310
FROM   edgewood_landuse_osm;

CREATE INDEX landuse_3310_gix
  ON landuse_3310 USING gist (geom_3310);

-- create road database
DROP TABLE IF EXISTS roads_3310;
CREATE TABLE roads_3310 AS
SELECT id,
       highway,
       ST_Transform(geom, 3310) AS geom_3310
FROM   edgewood_drive_roads
WHERE  highway IN ('residential','tertiary','secondary','primary');

CREATE INDEX roads_3310_gix
  ON roads_3310 USING gist (geom_3310);

-- build 1 mile buffer around every sensor
DROP TABLE IF EXISTS sensor_buffers;
CREATE TABLE sensor_buffers AS
SELECT sensor_id,
       ST_Buffer(geom_3310, 1609.344) AS buffer_geom,
       ST_Area( ST_Buffer(geom_3310, 1609.344) ) AS buffer_area_m2
FROM   sensors_3310;

CREATE INDEX sensor_buffers_gix
  ON sensor_buffers USING gist (buffer_geom);

------------------------------------------------------------------
-- landuse percentage in each buffer
-- select landuse features
DO $$
DECLARE
    lu_keep text[] := ARRAY[
        'residential','grass','retail','commercial','industrial','brownfield',
        'religious','construction','meadow','forest','farmland','recreation_ground',
        'plant_nursery','railway','cemetery','farmyard','military','village_green'
    ];
BEGIN
    -- nothing to do inside; just stores array for later SELECTs if you like
END $$;

DROP TABLE IF EXISTS landuse_recode_3310;
CREATE TABLE landuse_recode_3310 AS
SELECT
    id,
    -- 1-A.  分类映射：不在白名单里的一律给 'others'
    CASE
        WHEN landuse IN (
             'residential','grass','retail','commercial','industrial','brownfield',
             'religious','construction','meadow','forest','farmland','recreation_ground',
             'plant_nursery','railway','cemetery','farmyard','military','village_green'
        )
        THEN landuse
        ELSE 'others'
    END                       AS lu_class,
    -- 1-B.  投影到 3310
    ST_Transform(geom, 3310)  AS geom_3310
FROM edgewood_landuse_osm;

CREATE INDEX landuse_recode_gix
    ON landuse_recode_3310 USING gist (geom_3310);

-- 2-A.  交叠面积
DROP TABLE IF EXISTS lu_area_raw;
CREATE TABLE lu_area_raw AS
SELECT
    b.sensor_id,
    l.lu_class,
    SUM( ST_Area( ST_Intersection(b.buffer_geom, l.geom_3310) ) ) AS area_m2
FROM   sensor_buffers      b               -- ← 1-mile 缓冲区表
JOIN   landuse_recode_3310 l
  ON   ST_Intersects(b.buffer_geom, l.geom_3310)
GROUP BY b.sensor_id, l.lu_class;

-- 2-B.  转成百分比；比例型建议保留 double 或 numeric(6,4)
DROP VIEW IF EXISTS sensor_lu_road_summary;
DROP TABLE IF EXISTS landuse_pct;
CREATE TABLE landuse_pct AS
SELECT
    r.sensor_id,
    r.lu_class,
    ROUND( (r.area_m2 / s.buffer_area_m2)::numeric, 4 ) AS pct_of_buffer
FROM   lu_area_raw     r
JOIN   sensor_buffers  s USING (sensor_id);

-- change into long table
-- 确保启用 tablefunc
CREATE EXTENSION IF NOT EXISTS tablefunc;

DROP TABLE IF EXISTS landuse_wide;
CREATE TABLE landuse_wide AS
SELECT *
FROM crosstab(
    $$SELECT sensor_id, lu_class, pct_of_buffer
      FROM landuse_pct
      ORDER BY 1,2$$
) AS ct (
    sensor_id          bigint,
    residential        numeric,
    grass              numeric,
    retail             numeric,
    commercial         numeric,
    industrial         numeric,
    brownfield         numeric,
    religious          numeric,
    construction       numeric,
    meadow             numeric,
    forest             numeric,
    farmland           numeric,
    recreation_ground  numeric,
    plant_nursery      numeric,
    railway            numeric,
    cemetery           numeric,
    farmyard           numeric,
    military           numeric,
    village_green      numeric,
    others             numeric
);

------------------------------------------------------------------

-- calculate the closest distance from each type of road
DROP TABLE IF EXISTS road_dist;
CREATE TABLE road_dist AS
SELECT s.sensor_id,
       -- residential
       (SELECT MIN(ST_Distance(s.geom_3310, r.geom_3310))
          FROM roads_3310 r
         WHERE r.highway='residential') AS dist_res_m,
       -- tertiary
       (SELECT MIN(ST_Distance(s.geom_3310, r.geom_3310))
          FROM roads_3310 r
         WHERE r.highway='tertiary') AS dist_ter_m,
       -- secondary
       (SELECT MIN(ST_Distance(s.geom_3310, r.geom_3310))
          FROM roads_3310 r
         WHERE r.highway='secondary') AS dist_sec_m,
       -- primary
       (SELECT MIN(ST_Distance(s.geom_3310, r.geom_3310))
          FROM roads_3310 r
         WHERE r.highway='primary') AS dist_pri_m
FROM   sensors_3310 s;

-- build a database saving all data
DROP VIEW IF EXISTS sensor_lu_road_summary;

CREATE VIEW sensor_lu_road_summary AS
SELECT
    -- sensor_id 只出现一次
    d.sensor_id,
    d.dist_res_m,
    d.dist_ter_m,
    d.dist_sec_m,
    d.dist_pri_m,
    -- land-use 百分比全部展开
    w.residential,
    w.grass,
    w.retail,
    w.commercial,
    w.industrial,
    w.brownfield,
    w.religious,
    w.construction,
    w.meadow,
    w.forest,
    w.farmland,
    w.recreation_ground,
    w.plant_nursery,
    w.railway,
    w.cemetery,
    w.farmyard,
    w.military,
    w.village_green,
    w.others
FROM   road_dist     d
LEFT   JOIN landuse_wide w
       ON w.sensor_id = d.sensor_id        -- 用 ON … = …（或 USING）
ORDER  BY d.sensor_id;

-- export data as csv
SELECT * FROM sensor_lu_road_summary;

CREATE OR REPLACE VIEW sensor_lu_road_summary0 AS
SELECT
  d.sensor_id,
  d.dist_res_m,
  d.dist_ter_m,
  d.dist_sec_m,
  d.dist_pri_m,
  COALESCE(w.residential,      0) AS residential,
  COALESCE(w.grass,            0) AS grass,
  COALESCE(w.retail,           0) AS retail,
  COALESCE(w.commercial,       0) AS commercial,
  COALESCE(w.industrial,       0) AS industrial,
  COALESCE(w.brownfield,       0) AS brownfield,
  COALESCE(w.religious,        0) AS religious,
  COALESCE(w.construction,     0) AS construction,
  COALESCE(w.meadow,           0) AS meadow,
  COALESCE(w.forest,           0) AS forest,
  COALESCE(w.farmland,         0) AS farmland,
  COALESCE(w.recreation_ground,0) AS recreation_ground,
  COALESCE(w.plant_nursery,    0) AS plant_nursery,
  COALESCE(w.railway,          0) AS railway,
  COALESCE(w.cemetery,         0) AS cemetery,
  COALESCE(w.farmyard,         0) AS farmyard,
  COALESCE(w.military,         0) AS military,
  COALESCE(w.village_green,    0) AS village_green,
  COALESCE(w.others,           0) AS others
FROM road_dist d
LEFT JOIN landuse_wide w USING(sensor_id)
ORDER BY d.sensor_id;

SELECT * FROM sensor_lu_road_summary0;