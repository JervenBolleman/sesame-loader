import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;


public class StatementFromQueueIntoRepositoryPusher
    implements Runnable
{
	/**
     * 
     */
    private final loader loader;

    private final BlockingQueue<Statement> queue;

	private RepositoryConnection connection;
	private final int commitEveryStatements;

	public StatementFromQueueIntoRepositoryPusher(loader loader, BlockingQueue<Statement> queue, int commitEveryStatements,
	    RepositoryConnection connection)
	{
		super();
        this.loader = loader;
		this.queue = queue;
		this.commitEveryStatements = commitEveryStatements;
		this.connection = connection;
	}

	@Override
	public void run()
	{
		int counter = 0;
		if (this.loader.log.isDebugEnabled())
			this.loader.log.debug("Running into repository pusher");
		try
		{
			while (!this.loader.finished || !queue.isEmpty())
				counter = takeStatementFromQueueAddToConnection(counter);
			connection.commit();
		} catch (RepositoryException e)
		{
			this.loader.log.error("Pusher failed " + e.getMessage());
		}
	}

	private int takeStatementFromQueueAddToConnection(int counter)
	    throws RepositoryException
	{
		{
			try
			{
				final Statement st = queue.poll(100, TimeUnit.MILLISECONDS);
				if (st != null)
				{
					connection.add(st);
					counter++;
					if (counter % commitEveryStatements == 0)
					{
						if (this.loader.log.isDebugEnabled())
							this.loader.log.debug("Commiting into the connection pusher");
						connection.commit();
					}

				}
			} catch (InterruptedException e1)
			{
				Thread.currentThread().interrupt();
			}
		}
		return counter;
	}
}