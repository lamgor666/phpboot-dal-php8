<?php

namespace phpboot\dal\db;

final class Expression
{
    private string $expr;

    private function __construct(string $expr)
    {
        $this->expr = $expr;
    }

    public static function create(string $expr): self
    {
        return new self($expr);
    }

    public function getExpr(): string
    {
        return $this->expr;
    }
}
