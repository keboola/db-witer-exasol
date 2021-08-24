<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Tests\Traits;

use Doctrine;
use Keboola\DbWriter\Exasol\ExasolConnectionFactory;
use PDO;
use Psr\Log\NullLogger;

trait TestConnectionTrait
{
    public function createConnection(): PDO
    {
        $factory = new ExasolConnectionFactory(new NullLogger());
        $connection = $factory->create([
            'host' => (string) getenv('EXASOL_HOST'),
            'port' => (string) getenv('EXASOL_PORT'),
            'user' => (string) getenv('EXASOL_USERNAME'),
            '#password' => (string) getenv('EXASOL_PASSWORD'),
            'schema' => (string) getenv('EXASOL_SCHEMA'),
        ]);

        /** @var Doctrine\DBAL\Driver\PDO\Connection $pdo */
        $pdo = $connection->getWrappedConnection();
        return $pdo;
    }
}
