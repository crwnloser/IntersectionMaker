package org.example;

import java.io.*;

public class OSMImpoter {
    public void importOSM(String stylePath, String user, String password,
                          String dbName, String schemaName, String osmFilePath) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "osm2pgsql", "--style", stylePath, "--create", "--slim","-U", user, "-d", dbName,
                    "--schema=" + schemaName, osmFilePath
            );
            processBuilder.environment().put("PGPASSWORD", password);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
