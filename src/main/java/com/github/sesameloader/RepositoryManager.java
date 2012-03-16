package com.github.sesameloader;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;


public interface RepositoryManager
{

	RepositoryConnection getConnection()
	    throws RepositoryException;

	void shutDown()
	    throws SailException, RepositoryException;
	
	ValueFactory getValueFactory();
	
	/**
	 * Some repositories do not support more than one or a few threads, 
	 * or it is much faster to restrict them to one or a few threads.
	 * 
	 * This method enables the RepositoryManager to signal this to the 
	 * loader to avoid slow loading.
	 * 
	 * @return The maximum number of threads supported by this 
	 *     RepositoryManager, or 0 for an arbitrary number of threads.
	 */
	Integer getMaximumThreads();
}
