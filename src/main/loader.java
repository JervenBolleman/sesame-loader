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

import org.openrdf.OpenRDFException;
import org.openrdf.model.Statement;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.sail.SailException;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class loader
{
	private final BlockingQueue<Statement> queue = new ArrayBlockingQueue<Statement>(1000);
	Logger log = LoggerFactory.getLogger(loader.class);
	volatile boolean finished = false;
	private ExecutorService exec;
	final Repository repository;

	public loader(File dataDir, Integer commitXStatements, Integer threads) throws SailException, RepositoryException
	{
		repository = getRepository(dataDir);
		RepositoryConnection connection = getConnection(repository);
		createPushers(commitXStatements, threads, connection);
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
		OptionSpec<Integer> commitEveryXStatements = parser.accepts("commitInterval").withRequiredArg().required()
		    .ofType(Integer.class);
		OptionSpec<Integer> threads = parser.accepts("pushThreads").withRequiredArg().ofType(Integer.class).required();

		OptionSet options = parser.parse(args);
		if (options.has(infile) && options.has(dataFile) && options.has(baseUri) && options.has(commitEveryXStatements)
		    && options.has(threads))
		{
			final loader loader = new loader(options.valueOf(dataFile), options.valueOf(commitEveryXStatements),
			    options.valueOf(threads));
			loader.load(options.valueOf(infile), options.valueOf(baseUri));
		}
	}

	private RepositoryConnection getConnection(Repository repository)
	    throws RepositoryException
	{
		final RepositoryConnection connection = repository.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

	private Repository getRepository(File dataFileLocation)
	    throws RepositoryException
	{
	    Repository repository = new SailRepository(new NativeStore(dataFileLocation));
		repository.setDataDir(dataFileLocation);
		repository.initialize();
		log.debug("Repository initialized");
		return repository;
	}

	private void createPushers(Integer commitEveryXStatements, int threads, RepositoryConnection connection)
	{
		exec = Executors.newFixedThreadPool(threads);
		List<Future<?>> futures = new ArrayList<Future<?>>();
		for (int i = 0; i < threads; i++)
			futures.add(exec.submit(new StatementFromQueueIntoRepositoryPusher(this, queue, commitEveryXStatements,
			    connection)));
	}

	private void load(File file, String baseUri)
	    throws FileNotFoundException, IOException, RepositoryException
	{
		try
		{
			final String name = file.getName();
			if (name.endsWith(".gz"))
				load(new GZIPInputStream(new FileInputStream(file)), name.substring(0, name.length() - 3), baseUri);
			else
				load(new FileInputStream(file), name, baseUri);
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
			repository.shutDown();
		}
	}

	private void load(InputStream stream, String filename, String baseUri)
	    throws IOException
	{
		RDFFormat format = RDFFormat.forFileName(filename);
		RDFParser rdfParser = Rio.createParser(format);
		rdfParser.setRDFHandler(new StatementIntoQueuePusher(this, queue));
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
			finished = true;
			log.info(filename + "read");
		}
	}
}
