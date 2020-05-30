describe 'database' do
  before do
      `rm -rf test.db`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("./db test.db", "r+") do |pipe|
      commands.each do |command|
        begin
          pipe.puts command
        rescue Errno::EPIPE
          break
        end
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'faz o insert e retorna a row' do
    result = run_script([
      "insert 1 usuario usuario@example.com",
      "select",
      ".exit",
    ])
    
    expect(result).to match_array([
      "db > Executado.",
      "db > (1, usuario, usuario@example.com)",
      "Executado.",
      "db > ",
    ])
  end

  it 'mantem o dado depois de fechar a conexao' do
    result1 = run_script([
      "insert 1 usuario usuario@email.com",
      ".exit",
    ])
    expect(result1).to match_array([
      "db > Executado.",
      "db > ",
    ])
    result2 = run_script([
      "select",
      ".exit",
    ])
    expect(result2).to match_array([
      "db > (1, usuario, usuario@email.com)",
      "Executado.",
      "db > ",
    ])
  end

  it 'erro de tabela cheia' do
    script = (1..1401).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".exit"
    result = run_script(script)
    expect(result.last(2)).to match_array([
      "db > Executado.",
      "db > Falta implementar a divisao de nodes internos",
    ])
  end

  it 'permite inserir string no tamanho maximo' do
      long_username = "a"*32
      long_email = "a"*255
      script = [
        "insert 1 #{long_username} #{long_email}",
        "select",
        ".exit",
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > Executado.",
        "db > (1, #{long_username}, #{long_email})",
        "Executado.",
        "db > ",
      ])
  end

  it 'printa mensagem de erro se a string for maior que o limite' do
      long_username = "a"*33
      long_email = "a"*256
      script = [
        "insert 1 #{long_username} #{long_email}",
        "select",
        ".exit",
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > String ultrapassa o tamanho maximo para o campo.",
        "db > Executado.",
        "db > ",
      ])
  end

  it 'printa erro se o id for negativo' do
      script = [
        "insert -1 rodrigo foo@bar.com",
        "select",
        ".exit",
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > ID tem que ser um inteiro positivo.",
        "db > Executado.",
        "db > ",
      ])
  end

  it 'exibe as constantes' do
      script = [
          ".constants",
          ".exit",
      ]
      result = run_script(script)
      
      expect(result).to match_array([
          "db > Constantes:",
          "ROW_SIZE: 293",
          "COMMON_NODE_HEADER_SIZE: 6",
          "LEAF_NODE_HEADER_SIZE: 14",
          "LEAF_NODE_CELL_SIZE: 297",
          "LEAF_NODE_SPACE_FOR_CELLS: 4082",
          "LEAF_NODE_MAX_CELLS: 13",
          "db > ",
      ])
  end
  it 'exibe a estrutura de um node da btree' do
      script = [3, 1, 2].map do |i|
          "insert #{i} user#{i} person#{i}@example.com"
      end
      
      script << ".btree"
      script << ".exit"
      result = run_script(script)
  
      expect(result).to match_array([
          "db > Executado.",
          "db > Executado.",
          "db > Executado.",
          "db > Tree:",
          "- leaf (size 3)",
          "  - 1",
          "  - 2",
          "  - 3",
          "db > "
      ])
  end

  it 'exibe mensagem de erro para chave duplciada' do
      script = [
        "insert 1 user1 person1@example.com",
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > Executado.",
        "db > Erro: Chave duplicada.",
        "db > (1, user1, person1@example.com)",
        "Executado.",
        "db > ",
      ])
  end

  it 'printa a estrutura de 3-leaf-node btree' do
      script = (1..14).map do |i|
        "insert #{i} user#{i} person#{i}@example.com"
      end
      script << ".btree"
      script << "insert 15 user15 person15@example.com"
      script << ".exit"
      result = run_script(script)
  
      expect(result[14...(result.length)]).to match_array([
        "db > Tree:",
        "- internal (size 1)",
        "  - leaf (size 7)",
        "    - 1",
        "    - 2",
        "    - 3",
        "    - 4",
        "    - 5",
        "    - 6",
        "    - 7",
        "  - key 7",
        "  - leaf (size 7)",
        "    - 8",
        "    - 9",
        "    - 10",
        "    - 11",
        "    - 12",
        "    - 13",
        "    - 14",
        "db > Executado.",
        "db > ",
      ])
    end
end