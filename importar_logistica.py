import os
import re
import pandas as pd
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "150786",
    "database": "LOG_DIGITAL"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "logistica_entregas_geo.csv")


def ler_csv(caminho_arquivo: str) -> pd.DataFrame:
    for encoding in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            df = pd.read_csv(caminho_arquivo, sep=",", encoding=encoding)
            print(f"CSV lido com encoding: {encoding}")
            return df
        except Exception:
            pass

    raise ValueError("Não foi possível ler o arquivo CSV.")


def extrair_numero_cliente(valor):
    if pd.isna(valor):
        return None

    valor = str(valor).strip()
    numeros = re.sub(r"\D", "", valor)

    if numeros == "":
        return None

    return int(numeros)


def tratar_dados(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()

    print("\nColunas encontradas:")
    print(df.columns.tolist())

    # limpar textos
    colunas_texto = [
        "NomeCliente", "Cidade", "UF", "Produto",
        "CategoriaProduto", "Transportadora", "StatusEntrega"
    ]

    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    # transformar vazios em nulo
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # datas
    colunas_data = [
        "DataPedido", "DataEnvio", "DataEntregaPrevista",
        "DataEntregaReal", "DataPagamento"
    ]
    for col in colunas_data:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # numéricos normais
    colunas_numericas = [
        "PedidoID", "Quantidade", "ValorUnitario",
        "CustoFrete", "DistanciaKM", "AvaliacaoCliente"
    ]
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ClienteID especial: C022 -> 22
    if "ClienteID" in df.columns:
        df["ClienteID"] = df["ClienteID"].apply(extrair_numero_cliente)

    # UF
    if "UF" in df.columns:
        df["UF"] = df["UF"].astype("string").str.upper().str[:2]

    obrigatorias = [
        "PedidoID",
        "ClienteID",
        "NomeCliente",
        "Cidade",
        "UF",
        "Produto",
        "CategoriaProduto",
        "Transportadora",
        "Quantidade",
        "ValorUnitario",
        "CustoFrete",
        "DistanciaKM",
        "AvaliacaoCliente"
    ]

    obrigatorias_existentes = [c for c in obrigatorias if c in df.columns]

    print("\nNulos nas colunas obrigatórias:")
    print(df[obrigatorias_existentes].isna().sum())

    print("\nLinhas antes limpeza:", len(df))
    df = df.dropna(subset=obrigatorias_existentes).copy()
    print("Linhas depois limpeza:", len(df))

    if df.empty:
        raise ValueError("Depois da limpeza, o DataFrame ficou vazio.")

    # tipos finais
    df["PedidoID"] = df["PedidoID"].astype(int)
    df["ClienteID"] = df["ClienteID"].astype(int)
    df["Quantidade"] = df["Quantidade"].astype(int)
    df["AvaliacaoCliente"] = df["AvaliacaoCliente"].astype(int)

    return df


def conectar():
    return mysql.connector.connect(**DB_CONFIG)


def inserir_clientes(cursor, df):
    dados = (
        df[["ClienteID", "NomeCliente", "Cidade", "UF"]]
        .drop_duplicates(subset=["ClienteID"])
        .values.tolist()
    )

    cursor.executemany("""
        INSERT INTO Clientes (ClienteID, NomeCliente, Cidade, UF)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            NomeCliente = VALUES(NomeCliente),
            Cidade = VALUES(Cidade),
            UF = VALUES(UF)
    """, dados)


def inserir_transportadoras(cursor, df):
    dados = [(x,) for x in df["Transportadora"].drop_duplicates().tolist()]

    cursor.executemany("""
        INSERT INTO Transportadoras (NomeTransportadora)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE
            NomeTransportadora = VALUES(NomeTransportadora)
    """, dados)


def inserir_produtos(cursor, df):
    produtos = (
        df[["Produto", "CategoriaProduto", "ValorUnitario"]]
        .drop_duplicates(subset=["Produto"])
        .copy()
    )

    dados = [
        (row["Produto"], row["CategoriaProduto"], float(row["ValorUnitario"]))
        for _, row in produtos.iterrows()
    ]

    cursor.executemany("""
        INSERT INTO Produtos (NomeProduto, CategoriaProduto, ValorUnitario)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            CategoriaProduto = VALUES(CategoriaProduto),
            ValorUnitario = VALUES(ValorUnitario)
    """, dados)


def buscar_mapa(cursor, tabela, id_col, nome_col):
    cursor.execute(f"SELECT {id_col}, {nome_col} FROM {tabela}")
    return {nome: id_ for id_, nome in cursor.fetchall()}


def inserir_pedidos(cursor, df, mapa_transportadoras):
    pedidos = (
        df[
            [
                "PedidoID", "ClienteID", "Transportadora", "DataPedido",
                "DataEnvio", "DataEntregaPrevista", "DataEntregaReal",
                "StatusEntrega", "CustoFrete", "DistanciaKM", "AvaliacaoCliente"
            ]
        ]
        .drop_duplicates(subset=["PedidoID"])
        .copy()
    )

    dados = []

    for _, row in pedidos.iterrows():
        transportadora_id = mapa_transportadoras.get(row["Transportadora"])

        if transportadora_id is None:
            raise ValueError(f"Transportadora não encontrada: {row['Transportadora']}")

        dados.append((
            int(row["PedidoID"]),
            int(row["ClienteID"]),
            int(transportadora_id),
            row["DataPedido"].date() if pd.notna(row["DataPedido"]) else None,
            row["DataEnvio"].date() if pd.notna(row["DataEnvio"]) else None,
            row["DataEntregaPrevista"].date() if pd.notna(row["DataEntregaPrevista"]) else None,
            row["DataEntregaReal"].date() if pd.notna(row["DataEntregaReal"]) else None,
            row["StatusEntrega"],
            float(row["CustoFrete"]),
            float(row["DistanciaKM"]),
            int(row["AvaliacaoCliente"]),
        ))

    cursor.executemany("""
        INSERT INTO Pedidos (
            PedidoID, ClienteID, TransportadoraID, DataPedido, DataEnvio,
            DataEntregaPrevista, DataEntregaReal, StatusEntrega,
            CustoFrete, DistanciaKM, AvaliacaoCliente
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ClienteID = VALUES(ClienteID),
            TransportadoraID = VALUES(TransportadoraID),
            DataPedido = VALUES(DataPedido),
            DataEnvio = VALUES(DataEnvio),
            DataEntregaPrevista = VALUES(DataEntregaPrevista),
            DataEntregaReal = VALUES(DataEntregaReal),
            StatusEntrega = VALUES(StatusEntrega),
            CustoFrete = VALUES(CustoFrete),
            DistanciaKM = VALUES(DistanciaKM),
            AvaliacaoCliente = VALUES(AvaliacaoCliente)
    """, dados)


def limpar_itens_existentes(cursor, pedidos_ids):
    dados = [(int(pid),) for pid in pedidos_ids]
    cursor.executemany("DELETE FROM ItensPedido WHERE PedidoID = %s", dados)


def inserir_itens(cursor, df, mapa_produtos):
    dados = []

    for _, row in df.iterrows():
        produto_id = mapa_produtos.get(row["Produto"])

        if produto_id is None:
            raise ValueError(f"Produto não encontrado: {row['Produto']}")

        dados.append((
            int(row["PedidoID"]),
            int(produto_id),
            int(row["Quantidade"]),
            float(row["ValorUnitario"])
        ))

    cursor.executemany("""
        INSERT INTO ItensPedido (PedidoID, ProdutoID, Quantidade, ValorUnitario)
        VALUES (%s, %s, %s, %s)
    """, dados)


def main():
    conn = None
    cursor = None

    try:
        print("Lendo CSV...")
        df = ler_csv(CSV_PATH)

        print("Linhas lidas:", len(df))
        print(df.head())

        print("\nTratando dados...")
        df = tratar_dados(df)

        print("\nTotal de registros válidos:", len(df))

        conn = conectar()
        cursor = conn.cursor()

        print("Inserindo clientes...")
        inserir_clientes(cursor, df)
        conn.commit()

        print("Inserindo transportadoras...")
        inserir_transportadoras(cursor, df)
        conn.commit()

        print("Inserindo produtos...")
        inserir_produtos(cursor, df)
        conn.commit()

        mapa_produtos = buscar_mapa(cursor, "Produtos", "ProdutoID", "NomeProduto")
        mapa_transportadoras = buscar_mapa(cursor, "Transportadoras", "TransportadoraID", "NomeTransportadora")

        print("Inserindo pedidos...")
        inserir_pedidos(cursor, df, mapa_transportadoras)
        conn.commit()

        print("Limpando itens antigos...")
        limpar_itens_existentes(cursor, df["PedidoID"].unique().tolist())
        conn.commit()

        print("Inserindo itens...")
        inserir_itens(cursor, df, mapa_produtos)
        conn.commit()

        print("\nImportação concluída com sucesso.")

    except Exception as e:
        print("\nERRO:", e)
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("Conexão encerrada.")


if __name__ == "__main__":
    main()