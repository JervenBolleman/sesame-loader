package com.github.sesameloader;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;


public interface RepositoryManager
{

	public RepositoryConnection getConnection()
	    throws RepositoryException;

	public void shutDown()
	    throws SailException, RepositoryException;
}
