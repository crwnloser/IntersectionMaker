package org.example;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBReader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class IntersectionProcessor {
    private final JdbcTemplate jdbcTemplate;
    private final String tempTable;
    private final String mainTable;
    private static final int SRID = 3857; // Подставьте нужный SRID

    public IntersectionProcessor(String user, String password, String dbName, String tempSchemaName, String mainSchemaName) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/" + dbName);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.tempTable = tempSchemaName + ".planet_osm_line";
        this.mainTable = mainSchemaName + ".planet_osm_line";
    }

    public void processIntersections() {
        Set<Long> processedIds = new HashSet<>(); // Tracks already processed main line IDs
        Set<Long> deleteList = new HashSet<>(); // Tracks IDs of lines marked for deletion

        while (true) {
            Map<Long, List<Intersection>> intersections = findIntersections();
            if (intersections.isEmpty()) {
                break; // Exit when no intersections are found
            }

            for (Map.Entry<Long, List<Intersection>> entry : intersections.entrySet()) {
                long mainId = entry.getKey();
                if (processedIds.contains(mainId) || deleteList.contains(mainId)) {
                    continue; // Skip already processed or deleted lines
                }

                List<Intersection> intersectionList = entry.getValue();
                for (Intersection intersection : intersectionList) {
                    long tempId = intersection.lineId;
                    if (deleteList.contains(tempId) || deleteList.contains(mainId)) {
                        continue; // Skip already deleted lines
                    }

                    Geometry mainLine = intersection.mainLine;
                    Geometry tempLine = intersection.tempLine;
                    Geometry intersectionGeom = intersection.intersectionPoint;

                    if (mainLine.equals(tempLine)) {
                        deleteLine(tempId);
                        deleteList.add(tempId);
                    } else if (mainLine.contains(tempLine)) {
                        deleteLine(tempId);
                        deleteList.add(tempId);
                    } else if (mainLine.intersects(tempLine)) {
                        if (intersectionGeom instanceof Point point) {
                            addIntersectionNode(mainId, point, mainTable);
                            addIntersectionNode(tempId, point, tempTable);
                        } else if (intersectionGeom instanceof MultiPoint multiPoint) {
                            for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
                                Point p = (Point) multiPoint.getGeometryN(i);
                                addIntersectionNode(mainId, p, mainTable);
                                addIntersectionNode(tempId, p, tempTable);
                            }
                        } else {
                            splitLine(tempId, intersectionGeom);
                        }
                    } else if (mainLine.isWithinDistance(tempLine, 0.001)) {
                        mergeLines(mainId, tempId);
                        deleteList.add(tempId);
                    }
                }

                processedIds.add(mainId); // Mark the main line as processed
            }
        }
    }

    private Map<Long, List<Intersection>> findIntersections() {
        Map<Long, List<Intersection>> intersections = new HashMap<>();
        String sql = String.format(
                """
                SELECT m.osm_id AS main_id, t.osm_id AS temp_id,
                       ST_AsBinary(ST_Force2D(ST_Intersection(ST_Simplify(t.way, 10), ST_Simplify(m.way, 10)))) AS intersection_geom,
                       ST_AsBinary(ST_Force2D(t.way)) AS main_geom,
                       ST_AsBinary(ST_Force2D(m.way)) AS temp_geom
                FROM %s t, %s m
                WHERE ST_DWithin(t.way, m.way, 1000)
                  AND t.osm_id <> m.osm_id
                  AND (ST_Intersects(t.way, m.way) OR ST_DWithin(t.way, m.way, 0.001))
                """,
                tempTable, mainTable
        );
        System.out.println("Executing intersection query...");

        jdbcTemplate.query(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(1000);
            System.out.println("qq");
            System.out.println(tempTable);
            System.out.println(mainTable);
            return ps;
        }, rs -> {
            try {
                System.out.println("qq");
                long mainId = rs.getLong("main_id");
                long tempId = rs.getLong("temp_id");

                // Получаем бинарные данные геометрий
                byte[] intersectionBytes = rs.getBytes("intersection_geom");
                byte[] mainGeomBytes = rs.getBytes("main_geom");
                byte[] tempGeomBytes = rs.getBytes("temp_geom");

                // Преобразуем бинарные данные в объекты Geometry
                WKBReader wkbReader = new WKBReader();
                Geometry intersectionGeom = wkbReader.read(intersectionBytes);
                Geometry mainGeom = wkbReader.read(mainGeomBytes);
                Geometry tempGeom = wkbReader.read(tempGeomBytes);

                Intersection intersection = new Intersection(tempId, mainGeom, tempGeom, intersectionGeom);
                intersections.computeIfAbsent(mainId, k -> new ArrayList<>()).add(intersection);
            } catch (Exception e) {
                throw new RuntimeException("Error reading WKB from result set", e);
            }
        });

        System.out.println("Intersections found: " + intersections.size());
        return intersections;
    }

    private void mergeLines(long mainId, long tempId) {
        String sql = String.format(
                """
                UPDATE %s SET way = ST_Union(t1.way, t2.way)
                FROM %s t1, %s t2
                WHERE %s.osm_id = t1.osm_id
                  AND t1.osm_id = ?
                  AND t2.osm_id = ?
                """,
                tempTable, tempTable, tempTable, tempTable
        );
        jdbcTemplate.update(sql, mainId, tempId);
    }

    private void deleteLine(long lineId) {
        String sql = String.format("DELETE FROM %s WHERE osm_id = ?", tempTable);
        jdbcTemplate.update(sql, lineId);
    }

    private void splitLine(long tempId, Geometry intersectionGeom) {
        String sql = String.format(
                """
                SELECT ST_AsBinary(ST_Force2D(ST_Split(t.way, ST_GeomFromText(?, %d)))) AS split_geom
                FROM %s t WHERE t.osm_id = ?
                """,
                SRID, tempTable
        );

        jdbcTemplate.query(sql, rs -> {
            try {
                byte[] splitBytes = rs.getBytes("split_geom");
                WKBReader wkbReader = new WKBReader();
                Geometry splitGeom = wkbReader.read(splitBytes);

                if (splitGeom instanceof MultiLineString multiLine) {
                    for (int i = 0; i < multiLine.getNumGeometries(); i++) {
                        LineString line = (LineString) multiLine.getGeometryN(i);
                        insertNewLine(tempTable, line);
                    }
                } else if (splitGeom instanceof LineString line) {
                    insertNewLine(tempTable, line);
                }

                deleteLine(tempId);
            } catch (Exception e) {
                throw new RuntimeException("Error splitting line", e);
            }
        }, intersectionGeom.toText(), tempId);
    }

    private void insertNewLine(String table, LineString line) {
        String sql = String.format("INSERT INTO %s (way) VALUES (ST_GeomFromText(?, %d))", table, SRID);
        jdbcTemplate.update(sql, line.toText());
    }

    private void addIntersectionNode(long lineId, Point intersectPoint, String table) {
        String locateSql = String.format("SELECT ST_LineLocatePoint(way, ST_GeomFromText(?, %d)) FROM %s WHERE osm_id = ?", SRID, table);
        Double fraction = jdbcTemplate.queryForObject(locateSql, Double.class, intersectPoint.toText(), lineId);
        if (fraction == null) return;

        int pointIndex;
        if (fraction == 0.0) {
            pointIndex = 0;
        } else {
            String countSql = String.format(Locale.US,"SELECT ST_NumPoints(ST_LineSubstring(way, 0, %f)) FROM %s WHERE osm_id = ?", fraction, table);
            Integer numPoints = jdbcTemplate.queryForObject(countSql, Integer.class, lineId);
            if (numPoints == null) return;
            pointIndex = numPoints - 1;
        }

        String addPointSql = String.format("UPDATE %s SET way = ST_AddPoint(way, ST_GeomFromText(?, %d), %d) WHERE osm_id = ?", table, SRID, pointIndex);
        jdbcTemplate.update(addPointSql, intersectPoint.toText(), lineId);
    }

    private static class Intersection {
        private final Long lineId; // Temporary line ID
        private final Geometry mainLine; // Main line geometry
        private final Geometry tempLine; // Temporary line geometry
        private final Geometry intersectionPoint; // Geometry of the intersection

        public Intersection(Long lineId, Geometry mainLine, Geometry tempLine, Geometry intersectionPoint) {
            this.lineId = lineId;
            this.mainLine = mainLine;
            this.tempLine = tempLine;
            this.intersectionPoint = intersectionPoint;
        }
    }
}
