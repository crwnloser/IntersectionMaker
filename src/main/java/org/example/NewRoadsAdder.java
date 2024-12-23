package org.example;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NewRoadsAdder {
    public static void main( String[] args ) throws IOException {
        Properties config = loadProperties();

        String stylePath = config.getProperty("style.path");
        String user = config.getProperty("db.user");
        String password = config.getProperty("db.password");
        String dbName = config.getProperty("db.name");
        String tempSchemaName = config.getProperty("db.temp.schema");
        String mainSchemaName = config.getProperty("db.main.schema");
        String osmFilePath = config.getProperty("osm.file.path");

//        OSMImpoter importer  = new OSMImpoter();
//        importer.importOSM(stylePath, user, password, dbName, tempSchemaName, osmFilePath);
        IntersectionProcessor intersectionProcessor = new IntersectionProcessor(user, password, dbName, tempSchemaName, mainSchemaName);
        intersectionProcessor.processIntersections();
    }

    private static Properties loadProperties() throws IOException {
        try (InputStream inputStream = NewRoadsAdder.class.getClassLoader().getResourceAsStream("config")) {
            if (inputStream == null) {
                throw new FileNotFoundException("Файл config.txt не найден в resources");
            }
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

}
