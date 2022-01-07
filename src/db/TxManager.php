<?php

namespace phpboot\dal\db;

use PDO;

final class TxManager
{
    private PDO $pdo;

    private function __construct(PDO $pdo)
    {
        $this->pdo = $pdo;
    }

    public static function create(PDO $pdo): self
    {
        return new self($pdo);
    }

    public function getPdo(): PDO
    {
        return $this->pdo;
    }
}
