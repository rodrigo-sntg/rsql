# rsql

Reprodução de um banco de dados como SQLite utilizando apenas um array de rows não ordenadas.
Até o presente momento, a complexidade para: 
´´´
insersão - O(1)
delete - O(n)
select - O(n)
´´´

Os testes estão sendo feitos com ruby rspec, para executar basta:

´´´
bundle exec rspec
´´´