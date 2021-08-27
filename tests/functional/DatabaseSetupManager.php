<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\FunctionalTests;

use PDO;
use Keboola\ExasolWriter\Tests\Traits\Tables\IncrementalTableTrait;
use Keboola\ExasolWriter\Tests\Traits\Tables\SimpleTableTrait;

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
