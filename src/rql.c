#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>



// Definição do struct que irá receber os comandos
typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

// Definição dos tipos de nós
typedef enum {
  NODE_INTERNAL,
  NODE_LEAF
} NodeType;

// Definição do tipo de nós para o índice
typedef enum {
  INDEX_NODE_INTERNAL,
  INDEX_NODE_LEAF
} IndexNodeType;

// Tabela fake
#define COLUMN_EMAIL_SIZE 255
#define COLUMN_USERNAME_SIZE 32
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1]; // +1 para o character null no final do String
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// representação de uma linha da tabela
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id); // 4 bytes de ID
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username); // 32 bytes
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email); // 255 bytes
const uint32_t ID_OFFSET = 0; // offset 0 para alocação em memória
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE; // offset 4
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE; // offset 36
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // resto

// informações da tabela
const uint32_t PAGE_SIZE = 4096; // uma página inteira usada pela memoria virtual do SO
#define TABLE_MAX_PAGES 100

// Representação de uma página na memória
typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
} Pager;

// Representação da tabela
typedef struct {
  uint32_t root_page_num;
  Pager* pager;
} Table;

typedef enum { 
  EXECUTE_SUCCESS, 
  EXECUTE_DUPLICATE_KEY,
  EXECUTE_TABLE_FULL 
} ExecuteResult;

// enum de sucesso ou erro para comandos nao sql
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;


// enum de sucesso ou erro para comandos sql
typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;


// enum de comandos sql
typedef enum { 
    STATEMENT_INSERT, 
    STATEMENT_SELECT,
    STATEMENT_DELETE 
} StatementType;

// sql statement
typedef struct {
  StatementType type;
  Row row_to_insert; //usado na inserção
  uint32_t id_to_delete; // usado na exclusão
} Statement;

typedef struct{
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;

// Definição do struct para o índice
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
} IndexEntry;

typedef struct {
  IndexEntry entries[1000]; // Tamanho fixo para evitar problemas de escopo
  uint32_t size;
} Index;

// Inicialização do índice
Index* initialize_index() {
  Index* index = malloc(sizeof(Index));
  index->size = 0;
  return index;
}

// Definição do HEADER de um nó (node)
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Formato do HEADER de uma folha (leaf node)
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;
uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

/**
 * Layout do corpo da folha (leaf node body)
 * o corpo é composto por um array de celulas e 
 * cada celula é uma chave seguida de um valor (uma linha serializada)
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
//const uint32_t LEAF_NODE_MAX_CELLS = 3;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

// Layout do HEADER de um nó interno
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + 
                                                  INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Layout do corpo de um nó interno
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;


// Prototypes das funçõesa
void set_node_type(void* node, NodeType type);
void set_node_root(void* node, bool is_root);
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value);
void pager_flush(Pager* pager, uint32_t page_num);
void create_index(Table* table, Index* index);
Cursor* table_find(Table* table, uint32_t key);
void leaf_node_delete(Cursor* cursor, uint32_t key);
ExecuteResult execute_delete(Statement* statement, Table* table);
void* get_page(Pager* pager, uint32_t page_num);
NodeType get_node_type(void* node);
uint32_t* leaf_node_num_cells(void* node);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_child(void* node, uint32_t child_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
uint32_t* internal_node_right_child(void* node);
void print_leaf_node(void* node, uint32_t indentation_level);
void print_internal_node(Pager* pager, void* node, uint32_t indentation_level, uint32_t depth_limit);
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level, uint32_t depth_limit);


void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tentativa de recuperar uma pagina fora dos limites. %d > %d\n", page_num, TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // não encontrou no cache. Aloca memória e faz a leitura do arquivo
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // salva uma página parcial no fim do arquivo
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Erro ao ler o arquivo: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

// Funções de acesso aos campos dos nós
uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;
}

uint32_t* internal_node_num_keys(void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

uint32_t* internal_node_right_child(void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tentativa de acesso child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

NodeType get_node_type(void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

uint32_t* node_parent(void* node) {
  return node + PARENT_POINTER_OFFSET;
}
/**
 * Para um nó interno, o maior numero de chave é sempre a chave à direita
 * Para um nó folha, é o maior indice do nó.
 */
uint32_t get_node_max_key(void* node) {
  switch (get_node_type(node)) {
    case NODE_INTERNAL:
      return *internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
      return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
}

bool is_node_root(void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

// serialização dos dados
void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);  // usando strncpy para inicializar todas as posicoes da string em memoria
  strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // nó está cheio
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor->cell_num < num_cells) {
    // abrir espaço para uma nova célula
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void set_node_type(void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

// void indent(uint32_t level) {
//   for (uint32_t i = 0; i < level; i++) {
//     printf("  ");
//   }
// }

// void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
//   void* node = get_page(pager, page_num);
//   uint32_t num_keys, child;

//   switch (get_node_type(node)) {
//     case (NODE_LEAF):
//       num_keys = *leaf_node_num_cells(node);
//       indent(indentation_level);
//       printf("- leaf (size %d)\n", num_keys);
//       for (uint32_t i = 0; i < num_keys; i++) {
//         indent(indentation_level + 1);
//         printf("- %d\n", *leaf_node_key(node, i));
//       }
//       break;
//     case (NODE_INTERNAL):
//       num_keys = *internal_node_num_keys(node);
//       indent(indentation_level);
//       printf("- internal (size %d)\n", num_keys);
//       for (uint32_t i = 0; i < num_keys; i++) {
//         child = *internal_node_child(node, i);
//         print_tree(pager, child, indentation_level + 1);

//         indent(indentation_level + 1);
//         printf("- key %d\n", *internal_node_key(node, i));
//       }
//       child = *internal_node_right_child(node);
//       print_tree(pager, child, indentation_level + 1);
//       break;
//   }
// }

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

void print_leaf_node(void* node, uint32_t indentation_level) {
  uint32_t num_keys = *leaf_node_num_cells(node);
  indent(indentation_level);
  printf("- leaf (size %d)\n", num_keys);
  for (uint32_t i = 0; i < num_keys; i++) {
    if (i < 3 || i >= num_keys - 3) {
      indent(indentation_level + 1);
      printf("- %d\n", *leaf_node_key(node, i));
    } else if (i == 3) {
      indent(indentation_level + 1);
      printf("- ...\n");
    }
  }
}
void print_internal_node(Pager* pager, void* node, uint32_t indentation_level, uint32_t depth_limit) {
  uint32_t num_keys = *internal_node_num_keys(node);
  indent(indentation_level);
  printf("- internal (size %d)\n", num_keys);
  for (uint32_t i = 0; i < num_keys; i++) {
    uint32_t child = *internal_node_child(node, i);
    if (depth_limit > 0) {
      print_tree(pager, child, indentation_level + 1, depth_limit - 1);
    } else {
      indent(indentation_level + 1);
      printf("- ...\n");
    }

    indent(indentation_level + 1);
    printf("- key %d\n", *internal_node_key(node, i));
  }
  uint32_t right_child = *internal_node_right_child(node);
  if (depth_limit > 0) {
    print_tree(pager, right_child, indentation_level + 1, depth_limit - 1);
  } else {
    indent(indentation_level + 1);
    printf("- ...\n");
  }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level, uint32_t depth_limit) {
  void* node = get_page(pager, page_num);

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      print_leaf_node(node, indentation_level);
      break;
    case (NODE_INTERNAL):
      print_internal_node(pager, node, indentation_level, depth_limit);
      break;
  }
}

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// Dados do input
InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;

  // binary search
  uint32_t min_index = 0;
  uint32_t one_past_index = num_cells;
  while (one_past_index != min_index) {
    uint32_t index = (min_index + one_past_index) / 2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;

  return cursor;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
  /**
   * Retorna o indice do filho que deverá conter uma chave
  */
  uint32_t num_keys = *internal_node_num_keys(node);

/* Busca Binária */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; /* tem um filho a mais que chave */

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch (get_node_type(child)) {
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
  }
}



Cursor* table_find(Table* table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

Cursor* table_start(Table* table) {
  Cursor* cursor = table_find(table, 0);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);

  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  
  // if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
  //   cursor->end_of_table = true;
  //   /* Advance to next leaf node */
  //   uint32_t next_page_num = *leaf_node_next_leaf(node);
  //   if (next_page_num == 0) {
  //     /* This was rightmost leaf */
  //     cursor->end_of_table = true;
  //   } else {
  //     cursor->page_num = next_page_num;
  //     cursor->cell_num = 0;
  //   }
  // }
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    /* Advance to next leaf node */
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
        // This was the rightmost leaf
        cursor->end_of_table = true;
    } else {
        cursor->page_num = next_page_num;
        cursor->cell_num = 0;
    }
  }
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Erro ao fechar o banco de dados.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}


// iniciando o event loop
void print_prompt() { printf("rql > "); }

// faz a leitura do input do usuario com o getline
// void read_input(InputBuffer* input_buffer) {
//   ssize_t bytes_read =
//       getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

//   if (bytes_read <= 0) {
//     printf("Error reading input\n");
//     exit(EXIT_FAILURE);
//   }

//   // ignora novas linhas
//   input_buffer->input_length = bytes_read - 1;
//   input_buffer->buffer[bytes_read - 1] = 0;
// }

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Erro ao ler entrada\n");
    exit(EXIT_FAILURE);
  }

  // Ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// void pager_flush(Pager* pager, uint32_t page_num, uint32_t size){
void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tentativa de liberar uma página null.\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Erro ao buscar: %d\n");
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Erro ao escrever: %d\n");
    exit(EXIT_FAILURE);
  }
}

Index* username_index;

void print_index(Index* index) {
    printf("Index contents:\n");
    for (uint32_t i = 0; i < index->size; i++) {
        printf("%d: %s\n", index->entries[i].id, index->entries[i].username);
    }
}

// comandos não sql do usuário, iniciados sempre com .
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(table->pager, 0, 0, 3);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constantes:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".print_index") == 0) {
        print_index(username_index);
        return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

// Função para preparar a instrução de inserção
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  // Define o tipo da instrução como inserção
  statement->type = STATEMENT_INSERT;

  // Divide a entrada em palavras usando espaço como delimitador
  char* keyword = strtok(input_buffer->buffer, " ");
  char* id_string = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  // Verifica se todos os campos necessários foram fornecidos
  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR; // Retorna erro de sintaxe se algum campo estiver faltando
  }

  // Converte a string do ID para inteiro
  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID; // Retorna erro se o ID for negativo
  }

  // Verifica se o tamanho do username não excede o limite
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG; // Retorna erro se o username for muito longo
  }
  // Verifica se o tamanho do email não excede o limite
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG; // Retorna erro se o email for muito longo
  }

  // Copia os valores para a estrutura de inserção
  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  // Retorna sucesso na preparação da instrução de inserção
  return PREPARE_SUCCESS;
}


PrepareResult prepare_delete(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_DELETE;

  char* keyword = strtok(input_buffer->buffer, " ");
  char* id_string = strtok(NULL, " ");

  if (id_string == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }

  statement->id_to_delete = id;

  return PREPARE_SUCCESS;
}


// processador de comandos SQL
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
    return prepare_delete(input_buffer, statement);
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

/**
 * as novas paginas sempre irao para o final do arquivo do banco de dados
*/
uint32_t get_unused_page_num(Pager* pager) { 
  return pager->num_pages; 
}



void create_new_root(Table* table, uint32_t right_child_page_num) {
  /**
   * divide o nó raíz
   * A antiga raíz é copiada para uma nova página e se torna o filho da esquerda
   * O endereço do filho da direita é passado
   * Reinicializa a página raíz para contar o a o novo nó raíz
   * novo nó raíz aponta para os dois filhos
  */

  void* root = get_page(table->pager, table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);
  //The old root is copied to the left child so we can reuse the root page:

  /**
  * dados do filho da esquerda copiados para a nova raíz
  */

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

/**
 * inicializa a página raíz como um nó interno com 2 filhos
 * nó raíz é um novo nó interno com uma chave e 2 filhos
 */

  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}



void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  void* parent = get_page(table->pager, parent_page_num);
  void* child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
    // Dividir o nó interno se ele estiver cheio
    printf("Dividindo nó interno\n");
    uint32_t new_page_num = get_unused_page_num(table->pager);
    void* new_node = get_page(table->pager, new_page_num);
    initialize_internal_node(new_node);

    // Copiar metade das chaves e filhos para o novo nó
    uint32_t split_index = (INTERNAL_NODE_MAX_CELLS + 1) / 2;
    for (uint32_t i = split_index; i < INTERNAL_NODE_MAX_CELLS; i++) {
      *internal_node_child(new_node, i - split_index) = *internal_node_child(parent, i);
      *internal_node_key(new_node, i - split_index) = *internal_node_key(parent, i);
    }
    *internal_node_right_child(new_node) = *internal_node_right_child(parent);
    *internal_node_num_keys(new_node) = INTERNAL_NODE_MAX_CELLS - split_index;
    *internal_node_num_keys(parent) = split_index;

    // Se a chave a ser inserida é maior que a maior chave do nó pai original, insira no novo nó
    if (child_max_key > *internal_node_key(parent, split_index - 1)) {
      internal_node_insert(table, new_page_num, child_page_num);
    } else {
      internal_node_insert(table, parent_page_num, child_page_num);
    }

    // Criar um novo nó pai
    if (is_node_root(parent)) {
      create_new_root(table, new_page_num);
    } else {
      uint32_t parent_page_num = *node_parent(parent);
      uint32_t new_max = get_node_max_key(parent);
      void* parent_node = get_page(table->pager, parent_page_num);

      update_internal_node_key(parent_node, new_max, get_node_max_key(new_node));
      internal_node_insert(table, parent_page_num, new_page_num);
    }
    return;
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);
  void* right_child = get_page(table->pager, right_child_page_num);

  if (child_max_key > get_node_max_key(right_child)) {
    /* Substitui o filho direito */
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    /* Abre espaço para uma nova célula */
    for (uint32_t i = original_num_keys; i > index; i--) {
      void* destination = internal_node_cell(parent, i);
      void* source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}


void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
/**
  * Criar um node e move metade das celulas
  * Insere um novo valor em um dos dois nodes
  * atualiza o pai ou cria um novo pai
  */  
  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  /**
   * copia cada celula para seu lugar
   * todas as chaves existentes mais a nova chave devem ser
   * divididas igualmente entre nodes velhos (esquerda) e novos (direita)
   * iniciando pela direita, move cada chave para a posicao correta
   * 
  */  
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void* destination_node;
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    void* destination = leaf_node_cell(destination_node, index_within_node);

    if (i == cursor->cell_num) {
      serialize_row(value, leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }
  // Atualiza a contagem de celulas em cada header de node:

  /* Atualiza a contagem de celulas em ambos os nodes */
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  /**
   * Atualizar os parent nodes.
   * se o nó original for um nó raíz, não tem pai.
   * Nesse caso, cria um novo nó raíz para ser o pai.
   */

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(old_node);
    void* parent = get_page(cursor->table->pager, parent_page_num);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    return;
  }
}

// Adiciona uma entrada ao índice
void add_to_index(Index* index, uint32_t id, const char* username) {
  if (index->size >= 1000) { // Limite para evitar problemas
    printf("Índice está cheio\n");
    return;
  }
  index->entries[index->size].id = id;
  strcpy(index->entries[index->size].username, username);
  index->size++;
}


// Busca no índice por username
int search_index(Index* index, const char* username) {
  for (uint32_t i = 0; i < index->size; i++) {
    if (strcmp(index->entries[i].username, username) == 0) {
      return index->entries[i].id;
    }
  }
  return -1;
}

// Função para remover entrada do índice, se necessário (implementação opcional)
void remove_from_index(Index* index, uint32_t id) {
  for (uint32_t i = 0; i < index->size; i++) {
    if (index->entries[i].id == id) {
      index->entries[i] = index->entries[index->size - 1];
      index->size--;
      return;
    }
  }
}

void create_index(Table* table, Index* index) {
    Cursor* cursor = table_start(table);
    Row row;

    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        add_to_index(index, row.id, row.username);
        cursor_advance(cursor);
    }

    free(cursor);
}

void leaf_node_delete(Cursor* cursor, uint32_t key) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells == 0) {
        return; // No cells to delete
    }

    // Find the cell containing the key to delete
    uint32_t i;
    for (i = 0; i < num_cells; i++) {
        if (*leaf_node_key(node, i) == key) {
            break;
        }
    }

    if (i == num_cells) {
        return; // Key not found
    }

    // Remove the cell by shifting cells over
    for (uint32_t j = i; j < num_cells - 1; j++) {
        memcpy(leaf_node_cell(node, j), leaf_node_cell(node, j + 1), LEAF_NODE_CELL_SIZE);
    }

    (*leaf_node_num_cells(node))--;

    // Handle underflow if needed
    if (*leaf_node_num_cells(node) == 0 && cursor->page_num == cursor->table->root_page_num) {
        // If the root node is empty, free it
        void* root_node = get_page(cursor->table->pager, cursor->table->root_page_num);
        free(root_node);
        cursor->table->root_page_num = 0;
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }
}

ExecuteResult execute_insert_with_index(Statement* statement, Table* table) {
  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
  add_to_index(username_index, row_to_insert->id, row_to_insert->username);

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

// operação de select
ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor* cursor = table_start(table);
  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement* statement, Table* table) {
    Cursor* cursor = table_find(table, statement->id_to_delete);

    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == statement->id_to_delete) {
            leaf_node_delete(cursor, statement->id_to_delete);
            free(cursor);
            return EXECUTE_SUCCESS;
        }
    }

    free(cursor);
    return EXECUTE_SUCCESS; // Se a chave não for encontrada, ainda consideramos a operação bem-sucedida
}

// maquina virtual
ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert_with_index(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
    case (STATEMENT_DELETE):
      return execute_delete(statement, table);
  }
}

Pager* pager_open(const char* filename){
  int fd = open(filename, O_RDWR | // leitura e escrita
                          O_CREAT, // criar arquivo se nao existir
                          S_IWUSR | // permissao de escrita do usuario
                          S_IRUSR // permissao de leitura do usuari
                          );

  if (fd == -1) {
    printf("Não foi possível abrir o arquivo\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("O arquivo de banco de dados está corrompido.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {
    // banco de dados zerado, página 0 será o leaf node
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  return table;
}



// Função para executar uma seleção de registro baseado no username
ExecuteResult execute_select_by_username(Table* table, const char* username) {
  // Busca o ID correspondente ao username no índice
  int id = search_index(username_index, username);
  
  // Se o ID não for encontrado, informa que o registro não foi encontrado
  if (id == -1) {
    printf("Registro não encontrado.\n");
    return EXECUTE_SUCCESS;
  }

  // Cria um cursor para encontrar a linha na tabela pelo ID
  Cursor* cursor = table_find(table, id);
  
  // Declara uma estrutura Row para armazenar a linha desserializada
  Row row;
  
  // Desserializa a linha da tabela usando o cursor
  deserialize_row(cursor_value(cursor), &row);
  
  // Imprime a linha encontrada
  print_row(&row);

  // Libera a memória alocada para o cursor
  free(cursor);

  // Retorna sucesso na execução da seleção
  return EXECUTE_SUCCESS;
}


int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Necessário informar o nome do banco de dados.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);
  username_index = initialize_index();

  create_index(table, username_index);


  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Comando não reconhecido '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case (PREPARE_SYNTAX_ERROR):
        printf("Erro de sintaxe. Não foi possível interpretar a operação '%s'.\n", input_buffer->buffer);
        continue;
      case (PREPARE_STRING_TOO_LONG):
        printf("String ultrapassa o tamanho máximo para o campo.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Palavra chave não reconhecida '%s'.\n", input_buffer->buffer);
        continue;
      case (PREPARE_NEGATIVE_ID):
        printf("ID tem que ser um inteiro positivo.\n");
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executado.\n");
        break;
      case (EXECUTE_DUPLICATE_KEY):
        printf("Erro: Chave duplicada.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Erro: A tabela está cheia.\n");
        break;

      default:
        break;
    }
  }
}
