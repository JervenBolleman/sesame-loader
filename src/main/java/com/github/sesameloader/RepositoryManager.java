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
}
