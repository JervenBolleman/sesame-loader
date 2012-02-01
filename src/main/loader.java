/*
 * Copyright Swiss Institute of Bioinformatics (http://www.isb-sib.ch/) (c) 2012.
 *
 * Licensed under the Aduna BSD-style license.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sesameloader.RepositoryManager;
import com.github.sesameloader.StatementFromQueueIntoRepositoryPusher;
import com.github.sesameloader.StatementIntoQueuePusher;
import com.github.sesameloader.owlim.OwlimRepositoryManager;
import com.github.sesameloader.sesame.NativeRepositoryManager;

public class loader
{

    private final BlockingQueue<Statement> queue = new ArrayBlockingQueue<Statement>(1000);
    private final Logger log = LoggerFactory.getLogger(loader.class);
    volatile boolean finished = false;
    private ExecutorService exec;
    private final RepositoryManager manager;
    private final List<StatementFromQueueIntoRepositoryPusher> pushers = new ArrayList<StatementFromQueueIntoRepositoryPusher>();

    public loader(File dataDir, Integer commitXStatements, Integer threads, String providerType) throws SailException,
            RepositoryException
    {

        manager = getRepositoryManager(dataDir, providerType);
        createPushers(commitXStatements, threads, manager);
    }

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     * @throws SailException
     * @throws RepositoryException
     */
    public static void main(String[] args)
            throws FileNotFoundException, IOException, SailException, RepositoryException
    {

        OptionParser parser = new OptionParser();
        OptionSpec<File> infile = parser.accepts("infile").withRequiredArg().ofType(File.class).required();
        OptionSpec<File> dataFile = parser.accepts("dataFile").withRequiredArg().ofType(File.class).required();
        OptionSpec<String> baseUri = parser.accepts("baseUri").withRequiredArg().ofType(String.class).required();
        OptionSpec<Integer> commitEveryXStatements = parser.accepts("commitInterval").withRequiredArg().required().ofType(Integer.class);
        OptionSpec<Integer> threads = parser.accepts("pushThreads").withRequiredArg().ofType(Integer.class).required();
        OptionSpec<String> dataBaseProvider = parser.accepts("databaseProvider").withRequiredArg().ofType(String.class).required();

        OptionSet options = parser.parse(args);
        if (options.has(infile) && options.has(dataFile) && options.has(baseUri) && options.has(commitEveryXStatements)
                && options.has(threads))
        {
            final loader loader = new loader(options.valueOf(dataFile), options.valueOf(commitEveryXStatements),
                    options.valueOf(threads), options.valueOf(dataBaseProvider));
            loader.load(options.valueOf(infile), options.valueOf(baseUri));
        }
    }

    private RepositoryManager getRepositoryManager(File dataFileLocation, String databaseProvider)
            throws RepositoryException, SailException
    {

        if ("native".equalsIgnoreCase(databaseProvider))
            return new NativeRepositoryManager(dataFileLocation);
        else if ("owlim".equalsIgnoreCase(databaseProvider))
            return new OwlimRepositoryManager(dataFileLocation);
        else
            throw new RuntimeException("Don't know databaseProvider");
    }

    private void createPushers(Integer commitEveryXStatements, int threads, RepositoryManager connection)
            throws RepositoryException
    {
        exec = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++)
        {
            final StatementFromQueueIntoRepositoryPusher statementFromQueueIntoRepositoryPusher = new StatementFromQueueIntoRepositoryPusher(queue, commitEveryXStatements,
                    connection);
            pushers.add(statementFromQueueIntoRepositoryPusher);
            exec.submit(statementFromQueueIntoRepositoryPusher);
        }
    }

    private void load(File file, String baseUri)
            throws FileNotFoundException, IOException, RepositoryException, SailException
    {

        try
        {
            if (file.isDirectory())
                for (File infile : file.listFiles())
                    loadFile(infile, baseUri);
            else
                loadFile(file, baseUri);
            for (StatementFromQueueIntoRepositoryPusher pusher:pushers)
                pusher.setFinished(true);
            exec.shutdown();
            while (!exec.isTerminated())
                try
                {
                    exec.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
        } finally
        {
            manager.shutDown();
        }
    }

    private void loadFile(File file, String baseUri)
            throws FileNotFoundException, IOException, RepositoryException, SailException
    {
        final String name = file.getName();
        if (name.endsWith(".gz"))
            load(new GZIPInputStream(new FileInputStream(file)), name.substring(0, name.length() - 3), baseUri);
        else
            load(new FileInputStream(file), name, baseUri);
    }

    private void load(InputStream stream, String filename, String baseUri)
            throws IOException, RepositoryException
    {
        RDFFormat format = RDFFormat.forFileName(filename);
        RDFParser rdfParser = Rio.createParser(format);
        rdfParser.setValueFactory(manager.getConnection().getValueFactory());
        rdfParser.setRDFHandler(new StatementIntoQueuePusher(queue));
        try
        {
            rdfParser.parse(stream, baseUri);
        } catch (RDFParseException e)
        {
            log.error(e.getMessage());
        } catch (RDFHandlerException e)
        {
            log.error(e.getMessage());
        } finally
        {
            log.info(filename + "read");
        }
    }
}
