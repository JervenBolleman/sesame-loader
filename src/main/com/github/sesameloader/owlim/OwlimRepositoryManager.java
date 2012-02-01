package com.github.sesameloader.owlim;

import java.io.File;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ontotext.trree.OwlimSchemaRepository;
import com.github.sesameloader.RepositoryManager;
import java.util.concurrent.TimeUnit;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

public class OwlimRepositoryManager
        implements RepositoryManager
{

    private final Logger log = LoggerFactory.getLogger(OwlimRepositoryManager.class);
    private OwlimSchemaRepository repository;

    public OwlimRepositoryManager(File dataFileLocation) throws RepositoryException, SailException
    {
        repository = new OwlimSchemaRepository();
        repository.setDataDir(dataFileLocation);
        repository.initialize();
        log.debug("Repository initialized");
    }

    @Override
    public RepositoryConnection getConnection()
            throws RepositoryException
    {
        final SailRepositoryConnection connection = new SailRepository(repository).getConnection();
        connection.setAutoCommit(false);
        return connection;
//        throw new UnsupportedOperationException("OWLIM not free or available in maven");
    }

    @Override
    public void shutDown()
            throws SailException
    {
        repository.shutDown();
        while (repository.isShuttingDown())
            try
            {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e)
            {
                Thread.interrupted();
            }
    }
}
