package com.github.sesameloader;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatementFromQueueIntoRepositoryPusher
    implements Runnable
{

	private final BlockingQueue<Statement> queue;

	private final RepositoryConnection connection;
	private final int commitEveryStatements;
    private volatile boolean finished = false;
	private final CountDownLatch isDone;

	private Logger log = LoggerFactory.getLogger(StatementFromQueueIntoRepositoryPusher.class);

    private Resource[] contexts;

	public StatementFromQueueIntoRepositoryPusher(BlockingQueue<Statement> queue, int commitEveryStatements,
	    RepositoryManager manager, CountDownLatch isDone, Resource... contexts) throws RepositoryException
	{
		super();
		this.queue = queue;
		this.commitEveryStatements = commitEveryStatements;
		this.connection = manager.getConnection();
		this.contexts = contexts;
        this.isDone = isDone;
	}

	@Override
	public void run()
	{
		int counter = 0;
		if (log.isDebugEnabled())
			log.debug("Running into repository pusher");
		try
		{
			while (!this.finished || !queue.isEmpty())
				counter = takeStatementFromQueueAddToConnection(counter);
			connection.commit();
		} catch (RepositoryException e)
		{
			log.error("Pusher failed " + e.getMessage());
		}
		finally
		{
            try
            {
                connection.close();
            }
            catch(RepositoryException e)
            {
                log.error("Error closing connection to repository", e);
            }
            isDone.countDown();
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
					connection.add(st, contexts);
					counter++;
					if (counter % commitEveryStatements == 0)
					{
						if (log.isDebugEnabled())
							log.debug("Commiting into the connection pusher");
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

    public void setFinished(boolean b)
    {
        finished =true;
    }
}