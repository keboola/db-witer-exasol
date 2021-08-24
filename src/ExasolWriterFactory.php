<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol;

use Keboola\DbWriter\Exasol\Adapter\IAdapter;
use Keboola\DbWriter\Exasol\Adapter\NullAdapter;
use Keboola\DbWriter\WriterFactory;
use Psr\Log\LoggerInterface;

class ExasolWriterFactory extends WriterFactory
{
    private array $parameters;

    public function __construct(array $parameters)
    {
        $this->parameters = $parameters;
        parent::__construct($parameters);
    }

    public function create(LoggerInterface $logger, ?IAdapter $adapter = null): ExasolWriter
    {
        if (!$adapter) {
            $adapter = new NullAdapter();
        }

        return new ExasolWriter($this->parameters['db'], $logger, $adapter);
    }
}
