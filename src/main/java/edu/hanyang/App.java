package edu.hanyang;

import com.sun.net.httpserver.HttpHandler;
import edu.hanyang.httpserver.Handlers;
import edu.hanyang.httpserver.SimpleHttpServer;
import io.github.hyerica_bdml.indexer.*;
import edu.hanyang.services.ExternalSortService;
import edu.hanyang.services.IndexService;
import edu.hanyang.services.ServiceProvider;
import edu.hanyang.services.TokenizeService;
import edu.hanyang.utils.ExecuteQuery;
import edu.hanyang.utils.SqliteTable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class App {

    private Tokenizer tokenizer;
    private ExternalSort externalSort;
    private BPlusTree btree;
    private QueryProcess qp;
//    private String jarFilePath = null;
    private String jarFilePath = "lib/HanyangSE-submit-1.0.jar";

    public App() {

    }

    public void run() {
        Config.load();

        String dataDir = (String) Config.getValue("dataDir");
        String tempDir = (String) Config.getValue("tempDir");

        String tokenizedFilePath = (String) Config.getValue("tokenizedFilePath");
        String termIdsFilePath = (String) Config.getValue("termIdsFilePath");
        String titlesFilePath = (String) Config.getValue("titlesFilePath");
        String sortedFilePath = (String) Config.getValue("sortedFilePath");
        String postingFilePath = (String) Config.getValue("postingListFilePath");
        String metaFilePath = (String) Config.getValue("metaFilePath");
        String treeFilePath = (String) Config.getValue("treeFilePath");

        String dbName = (String) Config.getValue("server/dbName");

        int blockSize = Integer.parseInt((String) Config.getValue("blockSize"));
        int nBlocks = Integer.parseInt((String) Config.getValue("nBlocks"));
        int port = Integer.parseInt((String) Config.getValue("server/port"));


        try {
            // load submit files
            load(metaFilePath, treeFilePath, blockSize, nBlocks);

            // data preprocessing
            if (!Files.exists(Paths.get(postingFilePath))) {
                if (!Files.exists(Paths.get(sortedFilePath))) {
                    if (!Files.exists(Paths.get(tokenizedFilePath))) {
                        tokenizeData(
                                dataDir,
                                tokenizedFilePath,
                                termIdsFilePath,
                                titlesFilePath
                        );
                    }

                    sortData(
                            tokenizedFilePath,
                            sortedFilePath,
                            tempDir,
                            blockSize,
                            nBlocks
                    );
                }

                indexData(
                        sortedFilePath,
                        metaFilePath,
                        treeFilePath,
                        postingFilePath,
                        blockSize,
                        nBlocks
                );
            }

            btree.open(metaFilePath, treeFilePath, blockSize, nBlocks);

            if (!Files.exists(Paths.get(dbName))) {
                SqliteTable.init_conn(dbName);

                File[] files = Paths.get(dataDir).toFile().listFiles();
                System.out.println("Number of data files: " + files.length);

                for (File f: files) {
                    System.out.println(f.getName());

                    if (f.getName().endsWith(".csv")) {
                        try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                            // remove header
                            String line = reader.readLine();

                            while ((line = reader.readLine()) != null) {
                                String[] splited = line.split("\t");
                                if (splited.length != 4) continue;

                                int docid = Integer.parseInt(splited[1]);
                                String title = splited[2];
                                String content = splited[3];

                                SqliteTable.insert_doc(docid, content);
                            }
                        } catch (IOException exc) {
                            exc.printStackTrace();
                        }
                    }
                }

                SqliteTable.finalConn();
            }

            ServiceProvider.getTokenizeService().loadTokenFiles(termIdsFilePath, titlesFilePath);

            // starting server
            SqliteTable.init_conn(dbName);
            ExecuteQuery eq = new ExecuteQuery(
                    btree,
                    tokenizer,
                    postingFilePath,
                    blockSize,
                    nBlocks
            );
            HttpHandler handler = new Handlers.GetHandler(eq, qp);
            SimpleHttpServer server = new SimpleHttpServer();
            server.start(port, handler);

            System.out.println("Http server is started...");
            System.out.println("Enter e to quit");

            outerLoop: for (;;) {
                while (System.in.available() == 0) Thread.yield();
                switch (System.in.read()) {
                    case 'e': case 'E':
                        server.stop();
                        break outerLoop;
                    default:
                        break;
                }
            }

        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            try {
                btree.close();
                Config.close();
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    private void load(String metaFilePath,
                      String treeFilePath,
                      int blockSize,
                      int nBlocks) throws Exception {

        if (jarFilePath == null) {
            tokenizer = ServiceProvider.getTokenizeService().createNewTokenizer();
            externalSort = ServiceProvider.getExternalSortService().createNewExternalSort();
            btree = ServiceProvider.getIndexService().createNewBPlusTree(
                    metaFilePath,
                    treeFilePath,
                    blockSize,
                    nBlocks
            );
            qp = ServiceProvider.getQueryProcessService().createNewQueryProcess();
        } else {
            tokenizer = ServiceProvider.getTokenizeService().createNewTokenizer(jarFilePath);
            externalSort = ServiceProvider.getExternalSortService().createNewExternalSort(jarFilePath);
            btree = ServiceProvider.getIndexService().createNewBPlusTree(
                    jarFilePath,
                    metaFilePath,
                    treeFilePath,
                    blockSize,
                    nBlocks
            );
            qp = ServiceProvider.getQueryProcessService().createNewQueryProcess(jarFilePath);
        }
    }

    private void tokenizeData(String dataDir,
                              String tokenizedFilePath,
                              String termIdsFilePath,
                              String titlesFilePath) {
        System.out.println("Tokenizing data...");

        long startTime = System.currentTimeMillis();
        TokenizeService service = ServiceProvider.getTokenizeService();

        service.tokenize(
                tokenizer,
                dataDir,
                tokenizedFilePath,
                termIdsFilePath,
                titlesFilePath
        );
        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Tokenizing finished in " + duration + " secs.");
    }

    private void sortData(String tokenizedFilePath,
                          String sortedFilePath,
                          String tempDir,
                          int blockSize,
                          int nBlocks) {
        System.out.println("Sorting data...");

        long startTime = System.currentTimeMillis();
        ExternalSortService service = ServiceProvider.getExternalSortService();

        service.sort(
                externalSort,
                tokenizedFilePath,
                sortedFilePath,
                tempDir,
                blockSize,
                nBlocks
        );
        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Sorting finished in " + duration + " secs.");
    }

    private void indexData(String sortedFilePath,
                           String metaFilePath,
                           String treeFilePath,
                           String postingListFilePath,
                           int blockSize,
                           int nBlocks) throws Exception {
        System.out.println("Indexing data...");

        long startTime = System.currentTimeMillis();
        IndexService service = ServiceProvider.getIndexService();

        service.createNewInvertedList(
                btree,
                postingListFilePath,
                sortedFilePath,
                blockSize,
                nBlocks
        );
        btree.close();

        double duration = (double) (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Indexing finished in " + duration + " secs.");
    }

    public static void main(String[] args) {
        new App().run();
    }
}
