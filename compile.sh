#!/bin/bash
# gcc -o ./rql ./src/rql.c
# Diretório do código-fonte
SRC_DIR="src"
SRC_FILE="rql.c"

# Diretório de saída do executável
BIN_DIR="."
EXECUTABLE="rql"

# Compilação
gcc -o $BIN_DIR/$EXECUTABLE $SRC_DIR/$SRC_FILE

# Verificação de erro na compilação
if [ $? -eq 0 ]; then
    echo "Compilação bem-sucedida"
    # Executar o programa com o banco de dados de teste
    ./$BIN_DIR/$EXECUTABLE test.db
else
    echo "Erro na compilação"
fi
