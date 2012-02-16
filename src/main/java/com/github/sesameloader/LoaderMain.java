package com.github.sesameloader;
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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sesameloader.owlim.OwlimRepositoryManager;
import com.github.sesameloader.sesame.NativeRepositoryManager;

public class LoaderMain
{

    private final BlockingQueue<Statement> queue = new ArrayBlockingQueue<Statement>(1000);
    private final Logger log = LoggerFactory.getLogger(LoaderMain.class);
    volatile boolean finished = false;
    private ExecutorService exec;
    private final RepositoryManager manager;
    private final List<StatementFromQueueIntoRepositoryPusher> pushers = new ArrayList<StatementFromQueueIntoRepositoryPusher>();

    /**
     * Creates an instance of the LoaderMain class for a single bulk loading process using the given data directory as the repository location. 
     * 
     * @param dataDir The directory where the repository keeps its data files.
     * @param providerType The type of the repository to be used. Currently support "native" and "owlim" as values.
     * @param commitXStatements The number of statements to commit in each transaction.
     * @param threads The number of threads to use for loading.
     * @param contexts The contexts to put the statements into.
     * @throws SailException If there is a Sail exception thrown during the creation of the repository.
     * @throws RepositoryException If there is a Repository exception thrown during the creation of the repository.
     */
    public LoaderMain(File dataDir, String providerType, Integer commitXStatements, Integer threads, Resource... contexts) throws SailException,
            RepositoryException
    {
        this(getRepositoryManager(dataDir, providerType), commitXStatements, threads, contexts);
    }

    /**
     * Creates an instance of the LoaderMain class for a single bulk loading process using the given repository manager to access the repository.
     * 
     * @param nextManager The repository manager to use when accessing the repository.
     * @param commitXStatements The number of statements to commit in each transaction.
     * @param threads The number of threads to use for loading.
     * @param contexts The contexts to put the statements into.
     * @throws SailException If there is a Sail exception thrown during the creation of the repository.
     * @throws RepositoryException If there is a Repository exception thrown during the creation of the repository.
     */
    public LoaderMain(RepositoryManager nextManager, Integer commitXStatements, Integer threads, Resource... contexts) throws SailException, RepositoryException
    {
        this.manager = nextManager;
        createPushers(commitXStatements, threads, manager, contexts);
    }
    
    /**
     * The main method for this class when run from the command line.
     * 
     * Expects the following arguments:
     * 
     * infile : The file or directory to load.
     * dataFile : The location of the repository on the file system.
     * baseUri : The base URI for all of the files that are being loaded.
     * commitInterval : The number of statements to aggregate into a single transaction when loading.
     * pushThreads : The number of threads to use when loading the repository.
     * databaseProvider : The type of the repository. Currently we support two values for this field, "native" for a Sesame Native repository and "owlim" for an OwlimSchemaRepository.
     * 
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
            RepositoryManager repositoryManager = LoaderMain.getRepositoryManager(options.valueOf(dataFile), options.valueOf(dataBaseProvider));
            
            try
            {
                final LoaderMain loader = new LoaderMain(repositoryManager,
                        options.valueOf(commitEveryXStatements), options.valueOf(threads));
                loader.load(options.valueOf(infile), options.valueOf(baseUri));
            }
            finally
            {
                repositoryManager.shutDown();
            }
        }
    }

    public static RepositoryManager getRepositoryManager(File dataFileLocation, String databaseProvider)
            throws RepositoryException, SailException
    {

        if ("native".equalsIgnoreCase(databaseProvider))
            return new NativeRepositoryManager(dataFileLocation);
        else if ("owlim".equalsIgnoreCase(databaseProvider))
            return new OwlimRepositoryManager(dataFileLocation);
        else
            throw new RuntimeException("Don't know databaseProvider: "+databaseProvider);
    }

    /**
     * Creates the bulk loader threads using the given parameters and the given repository manager for connections to the repository.
     * 
     * @param connection The repository manager to use when accessing the repository.
     * @param commitXStatements The number of statements to commit in each transaction.
     * @param threads The number of threads to use for loading.
     * @param contexts The contexts to put the statements into.
     * @throws RepositoryException
     */
    private void createPushers(Integer commitEveryXStatements, int threads, RepositoryManager connection, Resource... contexts)
            throws RepositoryException
    {
        exec = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++)
        {
            final StatementFromQueueIntoRepositoryPusher statementFromQueueIntoRepositoryPusher = new StatementFromQueueIntoRepositoryPusher(queue, commitEveryXStatements,
                    connection, contexts);
            pushers.add(statementFromQueueIntoRepositoryPusher);
            exec.submit(statementFromQueueIntoRepositoryPusher);
        }
    }

    /**
     * This method loads RDF data in bulk using the given file. If the file is a directory, then every file in the directory will be loaded.
     * 
     * The format for the RDF files is determined for each file using the file extension. 
     * 
     * The parsers are chosen based on the current registered Sesame Rio parsers.
     * 
     * @param file The file or directory to load.
     * @param baseUri The base URI to use while loading the files.
     * @throws FileNotFoundException
     * @throws IOException
     * @throws RepositoryException
     * @throws SailException
     */
    public void load(File file, String baseUri)
            throws FileNotFoundException, IOException, RepositoryException, SailException
    {

        try
        {
            if (file.isDirectory())
                for (File infile : file.listFiles())
                    loadFileInternal(infile, baseUri);
            else
                loadFileInternal(file, baseUri);
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
            for (StatementFromQueueIntoRepositoryPusher pusher:pushers)
                pusher.setFinished(true);
        }
    }
    
    /**
     * This method loads RDF data from the given inputStream in bulk, using the given format and baseURI as guides.
     * 
     * Note that there must be a parser available in the list of currently loaded Sesame Rio parsers to match the given format.
     * 
     * @param stream The input stream containing RDF data
     * @param format The RDFFormat for the data in the input stream
     * @param baseUri The base URI to use for the load
     * @throws IOException Thrown if the stream fails for any reason.
     * @throws RepositoryException Thrown if there is an error related to the repository
     * @throws RDFParseException Thrown if the RDF data is not properly formed.
     * @throws RDFHandlerException Thrown if the bulk statement loader fails for any reason.
     * @throws SailException Thrown if an underlying Sail for the repository throws an exception.
     * @throws UnsupportedRDFormatException Thrown if a parser was not currently loaded to match the given format.
     */
    public void load(InputStream inputStream, RDFFormat format, String baseUri)
            throws IOException, RepositoryException, SailException, RDFParseException, RDFHandlerException, UnsupportedRDFormatException
    {

        try
        {
            loadInputStreamInternal(inputStream, format, baseUri);
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
            for (StatementFromQueueIntoRepositoryPusher pusher:pushers)
                pusher.setFinished(true);
        }
    }
    
    /**
     * Internal helper method that loads a single file using the given base URI.
     * 
     * This method logs as errors, but does not throw RDFParseException, RDFHandlerException and UnsupportedRDFormatException that occur during the loading process.
     * 
     * If you need these exceptions to be thrown, you can use loadInputStreamInternal directly.
     * 
     * @param file
     * @param baseUri
     * @throws FileNotFoundException
     * @throws IOException
     * @throws RepositoryException
     * @throws SailException
     */
    private void loadFileInternal(File file, String baseUri)
            throws FileNotFoundException, IOException, RepositoryException, SailException
    {
        final String name = file.getName();
        
        String shortFileName = name;
        InputStream inputStream = null;
        
        if (name.endsWith(".gz"))
        {
            inputStream = new GZIPInputStream(new FileInputStream(file));
            shortFileName = name.substring(0, name.length() - 3);
        }
        else
        {
            inputStream = new FileInputStream(file);
        }

        RDFFormat format = RDFFormat.forFileName(shortFileName);
        if (format == null)
        {
            log.error("Could not determine RDF format for filename="+shortFileName);
            return;
        }

        log.debug("parsing " + shortFileName + " using format " + format.toString());
        
        try
        {
            loadInputStreamInternal(inputStream, format, baseUri);
        } catch (RDFParseException e)
        {
            log.error(e.getMessage());
        } catch (RDFHandlerException e)
        {
            log.error(e.getMessage());
        } catch (UnsupportedRDFormatException e)
        {
            log.error(e.getMessage());
        }
        finally
        {
            log.info(shortFileName + " read");
        }
    }

    /**
     * Internal helper method that loads RDF in bulk from the given InputStream using the given format and base URI.
     * 
     * @param stream The input stream containing RDF data
     * @param format The RDFFormat for the data in the input stream
     * @param baseUri The base URI to use for the load
     * @throws IOException Thrown if the stream fails for any reason.
     * @throws RepositoryException Thrown if there is an error related to the repository
     * @throws RDFParseException Thrown if the RDF data is not properly formed.
     * @throws RDFHandlerException Thrown if the bulk statement loader fails for any reason.
     * @throws UnsupportedRDFormatException Thrown if a parser was not currently loaded to match the given format.
     */
    private void loadInputStreamInternal(InputStream stream, RDFFormat format, String baseUri)
            throws IOException, RepositoryException, RDFParseException, RDFHandlerException, UnsupportedRDFormatException
    {    
        RDFParser rdfParser = Rio.createParser(format);
        rdfParser.setValueFactory(manager.getValueFactory());
        rdfParser.setVerifyData(false);
        rdfParser.setPreserveBNodeIDs(false);
        rdfParser.setRDFHandler(new StatementIntoQueuePusher(queue));
        rdfParser.parse(stream, baseUri);
    }
}
