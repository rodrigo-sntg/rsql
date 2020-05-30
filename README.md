# RQL - SQLite em C

Uma reprodução do banco de dados SQLite.

Este projeto foi um estudo de b+ tree e o funcionamento de um banco de dados relacional utilizando b+tree.

Com a utilização de b+ tree, a complexidade das operações são as seguintes:

´´´
insert - O(log(n))
delete - O(log(n))
select - O(log(n))
´´´


Atualmente conta apenas com operação insert e select, para executar basta seguir:

```
./db teste.db
db > insert 1 rodrigo rodrigo@email
Executado.
db > select
(1, rodrigo, rodrigo@email)
Executado.
db > .btree
- leaf (size 1)
  - 1
db > .constants
Constantes:
ROW_SIZE: 293
COMMON_NODE_HEADER_SIZE: 6
LEAF_NODE_HEADER_SIZE: 14
LEAF_NODE_CELL_SIZE: 297
LEAF_NODE_SPACE_FOR_CELLS: 4082
LEAF_NODE_MAX_CELLS: 13
```

Os testes são feitos com rspec em ruby, para executar basta rodar:
```
bundle exec rspec
```