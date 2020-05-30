# rsql

Uma reprodução do banco de dados SQLite.

Nesta versão, passarei a utilizar uma árvore de nodes com b+ tree, para melhoria de performance das operações básicas do SQL.

Com essa alteração utilizando b+ tree, as operações terão complexidade:

´´´
insert - O(log(n))
delete - O(log(n))
select - O(log(n))
´´´


Os testes são feitos em ruby, para executar, rode o comando:

´´´
bundle exec rspec
´´´
