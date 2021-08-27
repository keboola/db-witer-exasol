<?php

declare(strict_types=1);

namespace Keboola\ExasolWriter\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ActionConfigDefinition extends BaseConfigDefinition
{
    public function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();
        $parametersNode
            ->ignoreExtraKeys(false)
            ->children()
                ->append(new DbNode())
            ->end();

        return $parametersNode;
    }
}
