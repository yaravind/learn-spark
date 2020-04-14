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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * <p>
 * The following website has lot of books do download to play with!
 * http://www.textfiles.com/etext/. This class downloads all the books related to FICTION category!
 * </p>
 * <p>
 * Execution times
 *     <ul>
 *         <li>40 threads: 15+/- secs, SWEET SPOT</li>
 *         <li>1 thread: 180+/- secs</li>
 *         <li>2 threads: 115 secs</li>
 *         <li>3 threads: 87 secs</li>
 *         <li>4 threads: 70+/- secs</li>
 *         <li>5 threads: 66+/- secs</li>
 *         <li>6 threads: 34+/- secs</li>
 *         <li>8 threads: 46+/- secs</li>
 *         <li>10 threads: 37+/- secs</li>
 *     </ul>
 *
 *
 * </p>
 */
public class FilesDownloader {

    public static final String FICTION_FILES_URL = "http://www.textfiles.com/etext/FICTION/";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Program expects 2 arguments:");
            System.out.println("1. Specify absolute directory path to save the downloaded files as a Program argument. Directory has to be empty!");
            System.out.println("2. Number of concurrent threads to download files parallelly. Entering '1' runs single threaded within the same process.");
            System.exit(0);
        }
        final String saveToFolderPath = args[0];
        final int numThreads = Integer.parseInt(args[1].trim());
        if (!isDirEmpty(saveToFolderPath)) {
            System.out.println("Directory (" + saveToFolderPath + ") is not empty. Empty the directory before you run the Program.");
            System.exit(0);
        }
        long startTime = System.nanoTime();
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
        System.out.println("Files will be downloaded to: " + saveToFolderPath + " with thread count: " + numThreads);

        long totalBytesDownloaded;
        if (numThreads == 1) {
            totalBytesDownloaded = downloadSingleThreaded(saveToFolderPath, links);
        } else {
            totalBytesDownloaded = downloadMultiThreaded(saveToFolderPath, links, numThreads);
        }

        long endtTime = System.nanoTime();
        System.out.println("Total bytes downloaded (Expected: 157,494,070 bytes): " + totalBytesDownloaded);
        System.out.println("Time taken to download (seconds): " + TimeUnit.NANOSECONDS.toSeconds(endtTime - startTime));
    }

    private static long downloadSingleThreaded(String saveToFolderPath, Elements links) {
        int downloadCounter = 0;
        long totalBytesDownloaded = 0;

        for (Element link : links) {
            String urlStr = link.attr("abs:href");
            String saveTo = saveToFolderPath + File.separator + link.text().trim();
            try {
                totalBytesDownloaded += downloadFile(urlStr, saveTo);
            } catch (IOException e) {
                System.err.println("Error downloading: " + urlStr + ". Continuing with next file");
                e.printStackTrace();
            }
            ++downloadCounter;
            System.out.println("Downloaded so far: " + downloadCounter);
        }
        return totalBytesDownloaded;
    }

    private static long downloadMultiThreaded(String saveToFolderPath, Elements links, int numThreads) {
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

        //int downloadCounter = 0;
        long totalBytesDownloaded = 0;

        //Prepare work
        List<SingleFileDownloaderTask> tasks = new ArrayList<>(links.size());
        for (Element link : links) {
            String urlStr = link.attr("abs:href");
            String saveTo = saveToFolderPath + File.separator + link.text().trim();
            tasks.add(new SingleFileDownloaderTask(urlStr, saveTo));
        }

        //Submit work
        System.out.println("Start");
        List<Future<Long>> futures = Collections.emptyList();
        try {
            futures = threadPool.invokeAll(tasks);
        } catch (InterruptedException err) {
            err.printStackTrace();
        } finally {
            threadPool.shutdown();
        }

        for (Future<Long> future : futures) {
            try {
                totalBytesDownloaded += future.get();
            } catch (CancellationException ce) { // if the computation was cancelled
                ce.printStackTrace();
            } catch (ExecutionException ee) { //if the computation threw an exception
                ee.printStackTrace();
            } catch (InterruptedException ie) { //if the current thread was interrupted while waiting
                Thread.currentThread().interrupt(); // ignore/reset
            }
        }

        System.out.println("Completed");

        //++downloadCounter;
        //System.out.println("Downloaded so far: " + downloadCounter);
        return totalBytesDownloaded;
    }

    public static long downloadFile(String urlStr, String saveTo) throws IOException {
        System.out.println("Downloading " + urlStr + " to " + saveTo);
        URL url = new URL(urlStr);
        try (InputStream in = url.openStream()) {
            return Files.copy(in, Paths.get(saveTo));
        }
    }

    public static class SingleFileDownloaderTask implements Callable<Long> {
        private final String urlStr;
        private final String saveTo;

        public SingleFileDownloaderTask(String urlStr, String saveTo) {
            this.urlStr = urlStr;
            this.saveTo = saveTo;
        }

        @Override
        public Long call() throws Exception {
            return downloadFile(urlStr, saveTo);
        }
    }

    private static boolean isDirEmpty(String strDirectoryPath) {
        Path directory = Paths.get(strDirectoryPath);
        boolean result = false;

        try {
            //Efficient because Files.list returns a lazily populated Stream
            Stream<Path> stream = Files.list(directory);
            result = stream.count() == 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
