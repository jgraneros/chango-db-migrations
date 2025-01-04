package org.chango;

import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@TopCommand
@Command(name = "export-data", mixinStandardHelpOptions = true,
        description = MigrationConstants.COMMAND_DESCRIPTION)
public class Migration implements Runnable{

    @Inject
    DataSource dataSource;

    @CommandLine.Option(names = {"-f", "--file"}, description = MigrationConstants.FILE_PATH_DESC)
    String filePath;

    @CommandLine.Option(names = {"-id"}, description = "ID principal de la entidad")
    String id;


    @Override
    public void run() {

        try (Connection conn = dataSource.getConnection()) {

            List<String> queries = leerSentenciasSQL(filePath);
            int counter = 1;

            for(String query: queries) {
                String queryWithEmpresaId = query.replace(":id", id);
                String fileName = extraerNombreTabla(queryWithEmpresaId);
                System.out.println("filename: " + fileName);
                exportarDatos(conn, queryWithEmpresaId, fileName);
                counter++;
            }
            System.out.println("Todas las consultas se ejecutaron correctamente...");
        } catch (SQLException | FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String extraerNombreTabla(String query) {
        String lowerQuery = query.toLowerCase();
        int fromIndex = lowerQuery.indexOf("from");

        if (fromIndex == -1) {
            throw new IllegalArgumentException("La consulta no contiene una cláusula FROM.");
        }

        String afterFrom = query.substring(fromIndex + 5).trim();
        String[] tokens = afterFrom.split("\\s+"); // Dividir por espacio en blanco
        return tokens[0].concat(".csv"); // El primer token después de FROM es el nombre de la tabla
    }


    private void exportarDatos(Connection conn, String query, String fileName) throws SQLException, IOException {

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query);
             PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {

            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                writer.print(rs.getMetaData().getColumnName(i));
                if (i < columnCount) writer.print(",");
            }
            writer.println();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    writer.print(rs.getString(i));
                    if (i < columnCount) writer.print(",");
                }
                writer.println();
            }
        }
        System.out.println("Datos exportados a " + fileName);

    }

    private List<String> leerSentenciasSQL(String filePath) throws Exception {
        List<String> queries = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder();
        try(BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                System.out.println("line: " + line);

                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }

                queryBuilder.append(line).append(" ");

                if (line.endsWith(";")) {
                    queries.add(queryBuilder.toString().trim());
                    queryBuilder.setLength(0);
                }

                if (queryBuilder.length() > 0) {
                    throw new Exception("Archivo contiene una sentencia SQL incompleta o malformada.");
                }

            }

            return queries;
        }
    }
}
