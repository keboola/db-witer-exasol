<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Adapter;

use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;

interface Adapter
{
    public function importToStagingTable(ExasolTableDefinition $table): void;
}
