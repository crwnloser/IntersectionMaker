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
    private static final int SRID = 3857;

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
        Set<Long> processedIds = new HashSet<>();
        Set<Long> deleteList = new HashSet<>();

        while (true) {
            Map<Long, List<Intersection>> intersections = findIntersections();
            if (intersections.isEmpty()) {
                System.out.println("No intersections found. Exiting loop.");
                break;
            }

            // Флаг, чтобы отследить, были ли изменения в этой итерации
            boolean changesHappened = false;

            System.out.println("=== New iteration ===");
            System.out.println("Found intersections for " + intersections.size() + " main lines.");

            for (Map.Entry<Long, List<Intersection>> entry : intersections.entrySet()) {
                long mainId = entry.getKey();

                // Проверяем, не обрабатывали ли мы уже mainId, и не удалён ли он
                if (processedIds.contains(mainId) || deleteList.contains(mainId)) {
                    System.out.println("Skipping mainId " + mainId + " (already processed or marked for deletion).");
                    continue;
                }

                List<Intersection> intersectionList = entry.getValue();
                System.out.println("Main line: " + mainId + ", intersections count: " + intersectionList.size());

                for (Intersection intersection : intersectionList) {
                    long tempId = intersection.lineId;

                    // Если tempId уже удалён или mainId удалён, пропускаем
                    if (deleteList.contains(tempId) || deleteList.contains(mainId)) {
                        System.out.println("Skipping tempId " + tempId + " (already deleted or mainId deleted).");
                        continue;
                    }

                    Geometry mainLine = intersection.mainLine;
                    Geometry tempLine = intersection.tempLine;
                    Geometry intersectionGeom = intersection.intersectionPoint;

                    // Логируем тип пересечения
                    System.out.println("Processing intersection between mainId=" + mainId
                            + " and tempId=" + tempId
                            + "; intersection geom type: " + intersectionGeom.getGeometryType());

                    // Проверяем случаи наложения
                    if (mainLine.equalsExact(tempLine)) {
                        // Полное совпадение
                        System.out.println("Lines are exactly equal. Deleting tempId " + tempId);
                        deleteLine(tempId);
                        deleteList.add(tempId);
                        changesHappened = true;

                    } else if (mainLine.contains(tempLine)) {
                        // mainLine полностью содержит tempLine
                        System.out.println("mainLine " + mainId + " fully contains tempId " + tempId + ". Deleting tempId.");
                        deleteLine(tempId);
                        deleteList.add(tempId);
                        changesHappened = true;

                    } else if (mainLine.intersects(tempLine)) {
                        // Частичное пересечение
                        System.out.println("mainLine " + mainId + " intersects tempLine " + tempId + ".");

                        if (intersectionGeom instanceof Point point) {
                            System.out.println("Intersection is a single Point. Adding intersection node...");
                            addIntersectionNode(mainId, point, mainTable);
                            addIntersectionNode(tempId, point, tempTable);
                            changesHappened = true;

                        } else if (intersectionGeom instanceof MultiPoint multiPoint) {
                            System.out.println("Intersection is a MultiPoint with " + multiPoint.getNumGeometries() + " points.");
                            for (int i = 0; i < multiPoint.getNumGeometries(); i++) {
                                Point p = (Point) multiPoint.getGeometryN(i);
                                addIntersectionNode(mainId, p, mainTable);
                                addIntersectionNode(tempId, p, tempTable);
                            }
                            changesHappened = true;

                        } else {
                            // Возможно, это LineString или MultiLineString
                            System.out.println("Intersection is not a Point (could be LineString). Splitting tempId " + tempId);
                            splitLine(tempId, intersectionGeom);
                            changesHappened = true;
                        }

                    } else if (mainLine.isWithinDistance(tempLine, 0.001)) {
                        // Почти пересекаются, очень близко
                        System.out.println("Lines are within 0.001 distance. Merging tempId " + tempId + " into mainId " + mainId);
                        mergeLines(mainId, tempId);
                        deleteList.add(tempId);
                        changesHappened = true;
                    }
                }

                // Отмечаем mainId как обработанный
                processedIds.add(mainId);
            }

            // Если в этой итерации не произошло никаких изменений — выходим, чтобы не застрять
            if (!changesHappened) {
                System.out.println("No changes happened in this iteration. Exiting loop to avoid infinite cycling.");
                break;
            }
        }
        System.out.println("Processing completed.");
    }

    /**
     * Находит пересечения (или близкое соседство) между дорогами из tempTable и mainTable.
     */
    private Map<Long, List<Intersection>> findIntersections() {
        Map<Long, List<Intersection>> intersections = new HashMap<>();

        String sql = String.format(
                """
                        SELECT m.osm_id AS main_id, t.osm_id AS temp_id,
                               ST_AsBinary(ST_Force2D(ST_Intersection(ST_Union(t.way), ST_Union(m.way)))) AS intersection_geom,
                               ST_AsBinary(ST_Force2D(ST_Union(t.way))) AS main_geom,
                               ST_AsBinary(ST_Force2D(ST_Union(m.way))) AS temp_geom
                        FROM %s t, %s m
                        WHERE ST_DWithin(t.way, m.way, 1000)
                          AND t.osm_id <> m.osm_id
                          AND t.highway IS NOT NULL
                          AND m.highway IS NOT NULL
                          AND (ST_Intersects(t.way, m.way) OR ST_DWithin(t.way, m.way, 0.001))
                          GROUP BY t.osm_id, m.osm_id
                        """,
                tempTable, mainTable
        );

        System.out.println("=== Executing intersection query ===");
        System.out.println("Query:\n" + sql);
        System.out.println("tempTable = " + tempTable + ", mainTable = " + mainTable);

        jdbcTemplate.query(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps.setFetchSize(1000);
            return ps;
        }, rs -> {
            try {
                long mainId = rs.getLong("main_id");
                long tempId = rs.getLong("temp_id");

                byte[] intersectionBytes = rs.getBytes("intersection_geom");
                byte[] mainGeomBytes = rs.getBytes("main_geom");
                byte[] tempGeomBytes = rs.getBytes("temp_geom");

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

        System.out.println("Intersections found for " + intersections.size() + " main lines.");
        return intersections;
    }

    /**
     * Сливает две линии (mainId и tempId) в tempTable через ST_Union.
     */
    private void mergeLines(long mainId, long tempId) {
        System.out.println("Merging lines: mainId=" + mainId + ", tempId=" + tempId);
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

    /**
     * Удаляет линию из tempTable по osm_id.
     */
    private void deleteLine(long lineId) {
        System.out.println("Deleting lineId " + lineId + " from " + tempTable);
        String sql = String.format("DELETE FROM %s WHERE osm_id = ?", tempTable);
        jdbcTemplate.update(sql, lineId);
    }

    /**
     * Разбивает линию (tempId) по геометрии пересечения intersectionGeom и вставляет новые части в tempTable.
     */
    private void splitLine(long tempId, Geometry intersectionGeom) {
        System.out.println("Splitting line tempId=" + tempId + " at geometry: " + intersectionGeom);
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

                // Если ST_Split вернул MultiLineString
                if (splitGeom instanceof MultiLineString multiLine) {
                    System.out.println("Split resulted in MultiLineString with " + multiLine.getNumGeometries() + " parts.");
                    for (int i = 0; i < multiLine.getNumGeometries(); i++) {
                        LineString line = (LineString) multiLine.getGeometryN(i);
                        insertNewLine(tempTable, line);
                    }
                }
                // Если одна линия
                else if (splitGeom instanceof LineString line) {
                    System.out.println("Split resulted in a single LineString.");
                    insertNewLine(tempTable, line);
                }

                // Удаляем исходную строку
                deleteLine(tempId);
            } catch (Exception e) {
                throw new RuntimeException("Error splitting line " + tempId, e);
            }
        }, intersectionGeom.toText(), tempId);
    }

    /**
     * Вставляет новую строку (LineString) в указанную таблицу.
     */
    private void insertNewLine(String table, LineString line) {
        System.out.println("Inserting new line into " + table + ": " + line.toText());
        String sql = String.format("INSERT INTO %s (way) VALUES (ST_GeomFromText(?, %d))", table, SRID);
        jdbcTemplate.update(sql, line.toText());
    }

    /**
     * Добавляет точку пересечения (Point) в линию (lineId) в указанной таблице.
     */
    private void addIntersectionNode(long lineId, Point intersectPoint, String table) {
        String locateSql = String.format(
                "SELECT ST_LineLocatePoint(way, ST_GeomFromText(?, %d)) FROM %s WHERE osm_id = ?",
                SRID, table
        );
        Double fraction = jdbcTemplate.queryForObject(locateSql, Double.class, intersectPoint.toText(), lineId);
        if (fraction == null) {
            System.out.println("No fraction found for lineId=" + lineId + " in table=" + table);
            return;
        }

        // Определяем, куда вставлять точку
        int pointIndex;
        if (fraction == 0.0) {
            pointIndex = 0;
        } else {
            // Считаем, сколько точек в подстроке
            String countSql = String.format(Locale.US,
                    "SELECT ST_NumPoints(ST_LineSubstring(way, 0, %f)) FROM %s WHERE osm_id = ?",
                    fraction, table
            );
            Integer numPoints = jdbcTemplate.queryForObject(countSql, Integer.class, lineId);
            if (numPoints == null) {
                System.out.println("NumPoints is null for lineId=" + lineId + " in table=" + table);
                return;
            }
            // Вставляем в конец подстроки
            pointIndex = numPoints - 1;
        }

        System.out.println("Adding intersection point to lineId=" + lineId + " at index=" + pointIndex);
        String addPointSql = String.format(
                "UPDATE %s SET way = ST_AddPoint(way, ST_GeomFromText(?, %d), %d) WHERE osm_id = ?",
                table, SRID, pointIndex
        );
        jdbcTemplate.update(addPointSql, intersectPoint.toText(), lineId);
    }

    /**
     * Вспомогательный класс для хранения данных о пересечении.
     */
    private static class Intersection {
        private final Long lineId;         // Temporary line ID
        private final Geometry mainLine;   // Main line geometry
        private final Geometry tempLine;   // Temporary line geometry
        private final Geometry intersectionPoint; // Geometry of the intersection

        public Intersection(Long lineId, Geometry mainLine, Geometry tempLine, Geometry intersectionPoint) {
            this.lineId = lineId;
            this.mainLine = mainLine;
            this.tempLine = tempLine;
            this.intersectionPoint = intersectionPoint;
        }
    }
}
