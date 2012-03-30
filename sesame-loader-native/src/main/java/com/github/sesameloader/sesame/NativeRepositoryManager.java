package com.github.sesameloader.sesame;

import java.io.File;

import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailException;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sesameloader.RepositoryManager;


public class NativeRepositoryManager
    implements RepositoryManager
{
	private final Logger log = LoggerFactory.getLogger(NativeRepositoryManager.class);
	private final Repository repository;

	public NativeRepositoryManager(File dataFileLocation) throws RepositoryException
	{
		super();
		repository = new SailRepository(new NativeStore(dataFileLocation));
		repository.setDataDir(dataFileLocation);
		repository.initialize();
		log.debug("Repository initialized");
	}

	@Override
	public RepositoryConnection getConnection()
	    throws RepositoryException
	{
		final RepositoryConnection connection = repository.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

	@Override
	public void shutDown()
	    throws SailException, RepositoryException
	{
		repository.shutDown();
	}

    @Override
    public ValueFactory getValueFactory()
    {
        return repository.getValueFactory();
    }

    @Override
    public Integer getMaximumThreads()
    {
        return 0;
    }
    
}
