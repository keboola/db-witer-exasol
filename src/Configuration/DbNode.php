<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Exasol\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class DbNode extends ArrayNodeDefinition
{
    public const NODE_NAME = 'db';

    public function __construct()
    {
        parent::__construct(self::NODE_NAME);
        $this->validate()->always(function ($v) {
            if (!is_numeric($v['port'])) {
                throw new InvalidConfigurationException(sprintf(
                    'Port "%s" has not a numeric value.',
                    $v['port']
                ));
            }
            $v['port'] = (int) $v['port'];
            return $v;
        })->end()->end();
        $this->isRequired();
        $this->init($this->children());
    }

    protected function init(NodeBuilder $builder): void
    {
        $builder
            ->scalarNode('host')
                ->isRequired()
            ->end()
            ->scalarNode('port')
                ->defaultValue(8563)
            ->end()
            ->scalarNode('user')
                ->isRequired()
            ->end()
            ->scalarNode('#password')
                ->isRequired()
            ->end()
            ->scalarNode('schema')
                ->isRequired()
            ->end();
    }
}
