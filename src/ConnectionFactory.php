<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter;

use Doctrine\DBAL\Connection;
use Keboola\ExasolWriter\Configuration\Config;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnection;
use Psr\Log\LoggerInterface;

class ConnectionFactory
{
    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function create(Config $config): Connection {

        $dsn = sprintf('%s:%s;EXASCHEMA=%s;', $config->getHost(), $config->getPort(), $config->getSchema());
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        return ExasolConnection::getConnection($dsn, $config->getUsername(), $config->getPassword());
    }
}
