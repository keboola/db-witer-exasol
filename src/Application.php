<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol;

use Keboola\DbWriter\Exasol\Adapter\S3Adapter;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Application as BaseApplication;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exasol\Adapter\IAdapter;
use Keboola\DbWriter\Exasol\Configuration\ActionConfigRowDefinition;
use Keboola\DbWriter\Exasol\Configuration\ConfigRowDefinition;
use Psr\Log\LoggerInterface;
use SplFileInfo;

class Application extends BaseApplication
{
    public const STORAGE_S3 = 's3';

    public function __construct(LoggerInterface $logger)
    {
        $dataDir = getenv('KBC_DATADIR') ?: '/data';
        $config = json_decode((string) file_get_contents($dataDir . '/config.json'), true);
        $config['parameters'] = $config['parameters'] ?? [];
        $config['parameters']['data_dir'] = $dataDir;

        $action = $config['action'] ?? 'run';
        if ($action === 'run') {
            $configDefinition = new ConfigRowDefinition();
        } else {
            $configDefinition = new ActionConfigRowDefinition();
        }

        parent::__construct($config, $logger, $configDefinition);

        $app = $this;
        $this['writer_factory'] = function () use ($app) {
            return $this->getWriterFactory($app['parameters']);
        };
    }

    public function runAction(): string
    {
        if (isset($this['parameters']['tables'])) {
            $tables = array_filter((array) $this['parameters']['tables'], function ($table) {
                return ($table['export']);
            });
            foreach ($tables as $key => $tableConfig) {
                $tables[$key] = $this->processRunAction($tableConfig);
            }
        } elseif (!isset($this['parameters']['export']) || $this['parameters']['export']) {
            $this->processRunAction($this['parameters']);
        }
        return 'Writer finished successfully';
    }

    private function processRunAction(array $tableConfig): array
    {
        $manifest = $this->getManifest($tableConfig['tableId']);
        $tableConfig['items'] = $this->reorderColumnsFromArray($manifest['columns'], $tableConfig['items']);

        if (empty($tableConfig['items'])) {
            return $tableConfig;
        }

        $adapter = $this->getAdapter($manifest);
        if (isset($tableConfig['incremental']) && $tableConfig['incremental']) {
            $this->writeIncrementalFromAdapter($tableConfig, $adapter);
        } else {
            $this->writeFullFromAdapter($tableConfig, $adapter);
        }

        return $tableConfig;
    }

    public function writeFull(SplFileInfo $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncremental(SplFileInfo $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncrementalFromAdapter(array $tableConfig, IAdapter $adapter): void
    {
        /** @var ExasolWriter $writer */
        $writer = $this['writer_factory']->create($this['logger'], $adapter);

        // create staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);
        $writer->createStaging($stageTable);
        $writer->writeFromAdapter($stageTable);

        // create destination table if not exists
        $writer->createIfNotExists($tableConfig);
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    public function writeFullFromAdapter(array $tableConfig, IAdapter $adapter): void
    {
        /** @var ExasolWriter $writer */
        $writer = $this['writer_factory']->create($this['logger'], $adapter);

        // create staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateStageName($tableConfig['dbName']);
        $writer->drop($stageTable['dbName']);
        $writer->create($stageTable);

        try {
            // create target table
            $writer->createIfNotExists($tableConfig);
            $writer->writeFromAdapter($stageTable);
            $writer->swapTables($tableConfig['dbName'], $stageTable['dbName']);
        } finally {
            $writer->drop($stageTable['dbName']);
        }
    }

    private function getManifest(string $tableId): array
    {
        $tableManifestPath = $this['parameters']['data_dir'] . '/in/tables/' . $tableId . '.csv.manifest';
        if (!file_exists($tableManifestPath)) {
            throw new UserException(sprintf(
                'Table "%s" in storage input mapping cannot be found.',
                $tableId
            ));
        }
        return json_decode(
            (string) file_get_contents($tableManifestPath),
            true
        );
    }

    private function getAdapter(array $manifest): IAdapter
    {
        if (isset($manifest[self::STORAGE_S3])) {
            return new S3Adapter($manifest[self::STORAGE_S3]);
        }
        throw new UserException('Unknown staging storage');
    }

    protected function getWriterFactory(array $parameters): ExasolWriterFactory
    {
        return new ExasolWriterFactory($parameters);
    }

    protected function reorderColumnsFromArray(array $csvHeader, array $items): array
    {
        $reordered = [];
        foreach ($csvHeader as $csvCol) {
            foreach ($items as $item) {
                if ($csvCol === $item['name']) {
                    $reordered[] = $item;
                }
            }
        }

        return $reordered;
    }
}
