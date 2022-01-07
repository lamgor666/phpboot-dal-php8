<?php

namespace phpboot\dal\pool;

use phpboot\common\traits\MapAbleTrait;

final class PoolInfo
{
    use MapAbleTrait;

    /**
     * @var int
     */
    private int $maxActive = 10;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private \Swoole\Atomic $currentActive;

    /**
     * @var \Swoole\Coroutine\Channel
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private \Swoole\Coroutine\Channel $connChan;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private \Swoole\Atomic $idleCheckRunning;

    /**
     * @var \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    private \Swoole\Atomic $closed;

    private function __construct(array $settings)
    {
        $this->fromMap($settings);
    }

    public static function create(array $settings): self
    {
        return new self($settings);
    }

    /**
     * @param int $maxActive
     */
    public function setMaxActive(int $maxActive): void
    {
        $this->maxActive = $maxActive;
    }

    /**
     * @return int
     */
    public function getMaxActive(): int
    {
        return $this->maxActive;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getCurrentActiveAtomic(): \Swoole\Atomic
    {
        return $this->currentActive;
    }

    /**
     * @return \Swoole\Coroutine\Channel
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getConnChan(): \Swoole\Coroutine\Channel
    {
        return $this->connChan;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getIdleCheckRunningAtomic(): \Swoole\Atomic
    {
        return $this->idleCheckRunning;
    }

    /**
     * @return \Swoole\Atomic
     * @noinspection PhpFullyQualifiedNameUsageInspection
     */
    public function getClosedAtomic(): \Swoole\Atomic
    {
        return $this->closed;
    }
}
