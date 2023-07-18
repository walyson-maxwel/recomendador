import os
import psycopg2
from dotenv import load_dotenv

class Recomendador:
    def __init__(self, cpf_usuario_alvo, num_usuarios_similares, num_cursos_para_analisar, config_db):
        self.cpf_usuario_alvo = cpf_usuario_alvo
        self.num_usuarios_similares = num_usuarios_similares
        self.num_cursos_para_analisar = num_cursos_para_analisar
        self.config_db = config_db

    def obter_cursos_concluidos_pelo_usuario_alvo(self):
        # Conecte-se ao banco de dados PostgreSQL
        cnx = psycopg2.connect(**self.config_db)
        cursor = cnx.cursor()

        # Obtenha os cursos mais recentemente concluídos pelo usuário alvo
        query = """
            SELECT acao
            FROM workspace.mvw_enapi_acoes
            WHERE cpf = %s
            ORDER BY dt_conclusao_raw DESC
            LIMIT %s
        """
        print(query % (self.cpf_usuario_alvo, self.num_cursos_para_analisar))
        cursor.execute(query, (self.cpf_usuario_alvo, self.num_cursos_para_analisar))
        cursos_concluidos_pelo_usuario_alvo = [row[0] for row in cursor]
        print(cursos_concluidos_pelo_usuario_alvo)

        # Feche o cursor e a conexão
        cursor.close()
        cnx.close()

        return cursos_concluidos_pelo_usuario_alvo

    def encontrar_usuarios_similares(self, cursos_concluidos_pelo_usuario_alvo):
        # Conecte-se ao banco de dados PostgreSQL
        cnx = psycopg2.connect(**self.config_db)
        cursor = cnx.cursor()

        # Encontre usuários que concluíram os mesmos cursos que o usuário alvo independentemente da ordem
        usuarios_similares = []
        for i in range(len(cursos_concluidos_pelo_usuario_alvo), 0, -1):
            query = """
                SELECT cpf
                FROM workspace.mvw_enapi_acoes
                WHERE cpf != %s AND acao IN ({})
                GROUP BY cpf
                HAVING COUNT(*) = %s
            """.format(','.join(['%s'] * i))
            params = (self.cpf_usuario_alvo,) + tuple(cursos_concluidos_pelo_usuario_alvo[:i]) + (i,)
            print(query % params)
            cursor.execute(query, params)
            usuarios_similares.extend([row[0] for row in cursor])
            print("Número de usuários similares encontrados:", len(usuarios_similares))
            if len(usuarios_similares) >= self.num_usuarios_similares:
                break

        usuarios_similares = list(set(usuarios_similares))[:self.num_usuarios_similares]

        # Feche o cursor e a conexão
        cursor.close()
        cnx.close()

        return usuarios_similares

    def recomendar_cursos(self, usuarios_similares, cursos_concluidos_pelo_usuario_alvo):
        # Conecte-se ao banco de dados PostgreSQL
        cnx = psycopg2.connect(**self.config_db)
        cursor = cnx.cursor()

        # Encontre cursos que os usuários similares concluíram mas que o usuário alvo não concluiu
        query = """
            SELECT acao, COUNT(*) as popularidade
            FROM workspace.mvw_enapi_acoes
            WHERE cpf IN ({}) AND acao NOT IN ({})
            GROUP BY acao
            ORDER BY popularidade DESC
            LIMIT 10
        """.format(','.join(['%s'] * len(usuarios_similares)), ','.join(['%s'] * len(cursos_concluidos_pelo_usuario_alvo)))
        params = tuple(usuarios_similares) + tuple(cursos_concluidos_pelo_usuario_alvo)
        print(query % params)
        cursor.execute(query, params)
        cursos_recomendados = [row[0] for row in cursor]

        # Feche o cursor e a conexão
        cursor.close()
        cnx.close()

        return cursos_recomendados

# Exemplo de uso
if __name__ == '__main__':
    load_dotenv()
    config_db = {
        'user': os.environ['DB_USER'],
        'password': os.environ['DB_PASSWORD'],
        'host': os.environ['DB_HOST'],
        'database': os.environ['DB_NAME']
    }
    recomendador = Recomendador(cpf_usuario_alvo='69584672134', num_usuarios_similares=100, num_cursos_para_analisar=10, config_db=config_db)
    cursos_concluidos_pelo_usuario_alvo = recomendador.obter_cursos_concluidos_pelo_usuario_alvo()
    usuarios_similares = recomendador.encontrar_usuarios_similares(cursos_concluidos_pelo_usuario_alvo)
    cursos_recomendados = recomendador.recomendar_cursos(usuarios_similares, cursos_concluidos_pelo_usuario_alvo)
