<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Connection\Exasol\ExasolConnection;
use Psr\Log\LoggerInterface;

class ExasolConnectionFactory
{
    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function create(array $dbParams): Connection {

        $dsn = sprintf('%s:%s;EXASCHEMA=%s;', $dbParams['host'], $dbParams['port'], $dbParams['schema']);
        $this->logger->info(sprintf('Connecting to %s', $dsn));
        return ExasolConnection::getConnection($dsn, $dbParams['user'], $dbParams['#password']);
    }
}
