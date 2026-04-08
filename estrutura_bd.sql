CREATE DATABASE LOG_DIGITAL;
USE LOG_DIGITAL;

CREATE TABLE Clientes (
    ClienteID INT PRIMARY KEY,
    NomeCliente VARCHAR(100),
    Cidade VARCHAR(100),
    UF CHAR(2)
);

CREATE TABLE Produtos (
    ProdutoID INT AUTO_INCREMENT PRIMARY KEY,
    NomeProduto VARCHAR(100) UNIQUE,
    CategoriaProduto VARCHAR(100),
    ValorUnitario DECIMAL(10,2)
);

CREATE TABLE Transportadoras (
    TransportadoraID INT AUTO_INCREMENT PRIMARY KEY,
    NomeTransportadora VARCHAR(100) UNIQUE
);

CREATE TABLE Pedidos (
    PedidoID INT PRIMARY KEY,
    ClienteID INT,
    TransportadoraID INT,
    DataPedido DATE,
    DataEnvio DATE,
    DataEntregaPrevista DATE,
    DataEntregaReal DATE,
    StatusEntrega VARCHAR(50),
    CustoFrete DECIMAL(10,2),
    DistanciaKM DECIMAL(10,2),
    AvaliacaoCliente INT,
    FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID),
    FOREIGN KEY (TransportadoraID) REFERENCES Transportadoras(TransportadoraID)
);

CREATE TABLE ItensPedido (
    ItemID INT AUTO_INCREMENT PRIMARY KEY,
    PedidoID INT,
    ProdutoID INT,
    Quantidade INT,
    ValorUnitario DECIMAL(10,2),
    FOREIGN KEY (PedidoID) REFERENCES Pedidos(PedidoID),
    FOREIGN KEY (ProdutoID) REFERENCES Produtos(ProdutoID)
);