package com.aravind.oss.eg.wordcount.java;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/***
 * The following website has lot of books do download to play with!
 * http://www.textfiles.com/etext/
 *
 * This class downloads all the books related to FICTION cateogory!
 */
public class FileDownloader {

    public static final String FICTION_FILES_URL = "http://www.textfiles.com/etext/FICTION/";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Specify absolute directory path to save the downloaded files as program argument");
            System.exit(0);
        }
        long startTime = System.nanoTime();
        final String downloadPath = args[0];
        Document doc = null;

        try {
            doc = Jsoup.connect(FICTION_FILES_URL).get();
        } catch (IOException e) {
            System.err.println("Unable to connect to: " + FICTION_FILES_URL);
            e.printStackTrace();
            System.exit(-1);
        }

        Elements links = doc.select("a[href]");

        System.out.println("Total files to download (Expected: 328): " + links.size());
        System.out.println("Files will be downloaded to: " + args[0]);

        long totalBytesDownloaded = 0;
        int downloadCounter = 0;

        for (Element link : links) {
            String urlStr = link.attr("abs:href");
            String saveToFileName = downloadPath + File.separator + link.text().trim();
            try {
                totalBytesDownloaded += downloadFile(urlStr, saveToFileName);
            } catch (IOException e) {
                System.err.println("Error downloading: " + urlStr + ". Continuing with next file");
                e.printStackTrace();
            }
            ++downloadCounter;
            System.out.println("Downloaded so far: " + downloadCounter);
        }
        long endtTime = System.nanoTime();
        System.out.println("Total bytes downloaded (Expected: 157,494,070 bytes): " + totalBytesDownloaded);
        System.out.println("Time taken to download (seconds): " + TimeUnit.NANOSECONDS.toSeconds(endtTime - startTime));
    }

    public static long downloadFile(String urlStr, String saveToFileName) throws IOException {
        System.out.println("Downloading: " + urlStr + " to " + saveToFileName);
        URL url = new URL(urlStr);
        try (InputStream in = url.openStream()) {
            return Files.copy(in, Paths.get(saveToFileName));
        }
    }
}
