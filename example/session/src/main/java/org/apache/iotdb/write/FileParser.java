package org.apache.iotdb.write;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileParser {

  public List<File> getAllFilesInDirectory(String directoryPath) {
    List<File> fileList = new ArrayList<>();
    File directory = new File(directoryPath);

    if (directory.exists() && directory.isDirectory()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isFile()) {
            fileList.add(file);
          } else if (file.isDirectory()) {
            fileList.addAll(getAllFilesInSubdirectory(file));
          }
        }
      }
    }

    return fileList;
  }

  private List<File> getAllFilesInSubdirectory(File subdirectory) {
    List<File> fileList = new ArrayList<>();
    File[] files = subdirectory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          fileList.add(file);
        } else if (file.isDirectory()) {
          fileList.addAll(getAllFilesInSubdirectory(file));
        }
      }
    }
    return fileList;
  }

  public static List<String[]> parseFileLines(File file) throws IOException {
    List<String[]> linesList = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split("[\\s\t]+"); // Split by spaces or tabs
        linesList.add(parts);
      }
    }

    return linesList;
  }

  public static List<String[]> parseFileLinesFiles(File file) throws IOException {
    List<String[]> linesList = new ArrayList<>();

    try (Stream<String> lines = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
      lines.forEach(
          x -> {
            String[] parts = x.split("[\\s\t]+"); // Split by spaces or tabs
            linesList.add(parts);
          });
    } catch (IOException e) {
      e.printStackTrace();
    }

    return linesList;
  }
}
