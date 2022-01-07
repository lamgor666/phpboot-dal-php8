<?php

namespace phpboot\dal\pool;

use Psr\Log\LoggerInterface;

interface PoolInterface
{
    public function inDebugMode(?bool $flag = null): bool;

    public function withLogger(LoggerInterface $logger): void;

    public function getLogger(): ?LoggerInterface;

    public function getPoolId(): string;

    public function getPoolType(): string;

    public function run(): void;

    public function take(int|float|null $timeout = null): mixed;

    public function release(mixed $conn): void;

    public function updateCurrentActive(int $num): void;

    public function destroy(int|string|null $timeout = null): void;
}
