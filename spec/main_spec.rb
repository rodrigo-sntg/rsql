describe 'database' do
    def run_script(commands)
      raw_output = nil
      IO.popen("./db", "r+") do |pipe|
        commands.each do |command|
            pipe.puts command
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

    it 'printa mensagem de erro com tabela cheia' do
        script = (1..1401).map do |i|
          "insert #{i} usuario#{i} usuario#{i}@example.com"
        end
        script << ".exit"
        result = run_script(script)
        expect(result[-2]).to eq('db > Erro: A tabela esta cheia. ')
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

    it 'mantem o dado depois de fechar a conexao' do
        result1 = run_script([
          "insert 1 usuario usuario@email.com",
          ".exit",
        ])
        expect(result1).to match_array([
          "db > executado.",
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
  end