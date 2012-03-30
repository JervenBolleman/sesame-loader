package com.github.sesameloader;

import java.util.concurrent.BlockingQueue;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;


public class StatementIntoQueuePusher
    extends RDFHandlerBase
{

	private final BlockingQueue<Statement> queue;

	public StatementIntoQueuePusher(BlockingQueue<Statement> queue)
	{
		super();
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