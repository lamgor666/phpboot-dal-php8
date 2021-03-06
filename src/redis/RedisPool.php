<?php

namespace phpboot\dal\redis;

use phpboot\dal\pool\PoolInfo;
use phpboot\dal\pool\PoolInterface;
use phpboot\dal\pool\PoolTrait;
use Redis;
use Throwable;

final class RedisPool implements PoolInterface
{
    use PoolTrait;

    private function __construct(int $workerId, PoolInfo $poolInfo, array $settings)
    {
        $settings['poolType'] = 'redis';
        $this->init($workerId, $poolInfo, $settings);
    }

    public static function create(int $workerId, PoolInfo $poolInfo, array $settings): self
    {
        return new self($workerId, $poolInfo, $settings);
    }

    private function newConnection(): ?Redis
    {
        $cfg = RedisConfig::loadCurrent($this->workerId);

        if (!($cfg instanceof RedisConfig) || !$cfg->isEnabled()) {
            return null;
        }

        try {
            $redis = RedisConnection::create($this->poolId, $cfg);
        } catch (Throwable) {
            $redis = null;
        }

        return $redis instanceof Redis ? $redis : null;
    }
}
