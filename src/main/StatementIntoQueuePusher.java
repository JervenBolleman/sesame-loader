import java.util.concurrent.BlockingQueue;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;


public class StatementIntoQueuePusher
    extends RDFHandlerBase
{

	/**
     * 
     */
    private final loader loader;
    private final BlockingQueue<Statement> queue;

	public StatementIntoQueuePusher(loader loader, BlockingQueue<Statement> queue)
	{
		super();
        this.loader = loader;
		this.queue = queue;
	}

	@Override
	public void handleStatement(Statement st)
	{
		try
		{
			queue.put(st);
		} catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
		}
	}
}