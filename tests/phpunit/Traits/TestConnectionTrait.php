<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Tests\Traits;

use Doctrine;
use Keboola\ExasolWriter\Configuration\ActionConfigDefinition;
use Keboola\ExasolWriter\Configuration\Config;
use Keboola\ExasolWriter\ConnectionFactory;
use PDO;
use Psr\Log\NullLogger;

trait TestConnectionTrait
{
    public function createConnection(): PDO
    {
        $factory = new ConnectionFactory(new NullLogger());
        $config = [
            'parameters' => [
                'db' => [
                    'host' => (string)getenv('EXASOL_HOST'),
                    'port' => (string)getenv('EXASOL_PORT'),
                    'user' => (string)getenv('EXASOL_USERNAME'),
                    '#password' => (string)getenv('EXASOL_PASSWORD'),
                    'schema' => (string)getenv('EXASOL_SCHEMA'),
                ],
            ],
        ];
        $connection = $factory->create(new Config($config, new ActionConfigDefinition()));

        /** @var Doctrine\DBAL\Driver\PDO\Connection $pdo */
        $pdo = $connection->getWrappedConnection();
        return $pdo;
    }
}
