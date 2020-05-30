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

    it 'exibe todas as linhas em arvore multi nivel' do
      script = []
      (1..15).each do |i|
        script << "insert #{i} user#{i} person#{i}@example.com"
      end
      script << "select"
      script << ".exit"
      result = run_script(script)
  
      expect(result[15...result.length]).to match_array([
        "db > (1, user1, person1@example.com)",
        "(2, user2, person2@example.com)",
        "(3, user3, person3@example.com)",
        "(4, user4, person4@example.com)",
        "(5, user5, person5@example.com)",
        "(6, user6, person6@example.com)",
        "(7, user7, person7@example.com)",
        "(8, user8, person8@example.com)",
        "(9, user9, person9@example.com)",
        "(10, user10, person10@example.com)",
        "(11, user11, person11@example.com)",
        "(12, user12, person12@example.com)",
        "(13, user13, person13@example.com)",
        "(14, user14, person14@example.com)",
        "(15, user15, person15@example.com)",
        "Executado.", 
        "db > ",
      ])
    end

    it 'allows printing out the structure of a 4-leaf-node btree' do
      script = [
        "insert 18 user18 person18@example.com",
        "insert 7 user7 person7@example.com",
        "insert 10 user10 person10@example.com",
        "insert 29 user29 person29@example.com",
        "insert 23 user23 person23@example.com",
        "insert 4 user4 person4@example.com",
        "insert 14 user14 person14@example.com",
        "insert 30 user30 person30@example.com",
        "insert 15 user15 person15@example.com",
        "insert 26 user26 person26@example.com",
        "insert 22 user22 person22@example.com",
        "insert 19 user19 person19@example.com",
        "insert 2 user2 person2@example.com",
        "insert 1 user1 person1@example.com",
        "insert 21 user21 person21@example.com",
        "insert 11 user11 person11@example.com",
        "insert 6 user6 person6@example.com",
        "insert 20 user20 person20@example.com",
        "insert 5 user5 person5@example.com",
        "insert 8 user8 person8@example.com",
        "insert 9 user9 person9@example.com",
        "insert 3 user3 person3@example.com",
        "insert 12 user12 person12@example.com",
        "insert 27 user27 person27@example.com",
        "insert 17 user17 person17@example.com",
        "insert 16 user16 person16@example.com",
        "insert 13 user13 person13@example.com",
        "insert 24 user24 person24@example.com",
        "insert 25 user25 person25@example.com",
        "insert 28 user28 person28@example.com",
        ".btree",
        ".exit",
      ]
      result = run_script(script)
  
      expect(result[30...(result.length)]).to match_array([
        "db > Tree:",
        "- internal (size 3)",
        "  - leaf (size 7)",
        "    - 1",
        "    - 2",
        "    - 3",
        "    - 4",
        "    - 5",
        "    - 6",
        "    - 7",
        "  - key 7",
        "  - leaf (size 8)",
        "    - 8",
        "    - 9",
        "    - 10",
        "    - 11",
        "    - 12",
        "    - 13",
        "    - 14",
        "    - 15",
        "  - key 15",
        "  - leaf (size 7)",
        "    - 16",
        "    - 17",
        "    - 18",
        "    - 19",
        "    - 20",
        "    - 21",
        "    - 22",
        "  - key 22",
        "  - leaf (size 8)",
        "    - 23",
        "    - 24",
        "    - 25",
        "    - 26",
        "    - 27",
        "    - 28",
        "    - 29",
        "    - 30",
        "db > ",
      ])
    end
end