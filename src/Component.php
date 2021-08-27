<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter;

use Keboola\Component\BaseComponent;
use Keboola\DbWriter\Exception\UserException;
use Keboola\ExasolWriter\Configuration\ActionConfigDefinition;
use Keboola\ExasolWriter\Configuration\Config;
use Keboola\ExasolWriter\Configuration\ConfigDefinition;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    public const ACTION_RUN = 'run';
    public const ACTION_TEST_CONNECTION = 'testConnection';

    private Writer $writer;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
        $this->writer = new Writer($this->getConfig(), $this->getDataDir(), $this->getLogger());
    }

    protected function run(): void
    {
        $this->writer->write();
    }

    protected function handleTestConnection(): array
    {
        try {
            $this->writer->testConnection();
        } catch (\Throwable $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }

        return ['status' => 'success'];
    }

    public function getConfig(): Config
    {
        $config = parent::getConfig();
        assert($config instanceof Config);
        return $config;
    }

    protected function getSyncActions(): array
    {
        return [
            self::ACTION_TEST_CONNECTION => 'handleTestConnection',
        ];
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        $action = $this->getRawConfig()['action'] ?? 'run';
        switch ($action) {
            case self::ACTION_RUN:
                return ConfigDefinition::class;

            case self::ACTION_TEST_CONNECTION:
                return ActionConfigDefinition::class;
            default:
                throw new UnexpectedValueException(sprintf('Unexpected action "%s"', $action));
        }
    }
}
