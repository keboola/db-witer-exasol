<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\FunctionalTests;

use PDO;
use Keboola\DbWriter\Exasol\Tests\Traits\Tables\IncrementalTableTrait;
use Keboola\DbWriter\Exasol\Tests\Traits\Tables\SimpleTableTrait;

class DatabaseSetupManager
{
    use SimpleTableTrait;
    use IncrementalTableTrait;

    protected PDO $connection;

    public function __construct(PDO $connection)
    {
        $this->connection = $connection;
    }

    public function getConnection(): PDO
    {
        return $this->connection;
    }
}
