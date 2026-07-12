# MiniShell Linux Distribuída

> Projeto desenvolvido para a disciplina **Computação Distribuída e Paralela**  
> **Professor:** Ronaldo Oikawa  
> **Autor:** Kauan dos Santos Loche

---

# Visão Geral

A MiniShell Distribuída implementa uma shell Linux simplificada capaz de executar comandos locais e distribuídos entre múltiplos nós conectados via TCP.

Cada instância da aplicação representa um **nó independente**, contendo:

- Servidor TCP
- Cliente TCP
- Gerenciador de peers
- Eleição de líder (Bully)
- Exclusão mútua distribuída
- Multicast confiável e ordenado
- Interpretador de comandos

Os nós cooperam para manter um ambiente distribuído com coordenação automática, replicação de comandos e sincronização de recursos.

---

# Arquitetura

```
Usuário
    │
    ▼
MiniShell (Prompt)
    │
    ▼
Command Parser
    │
    ▼
Node
 ├── Server TCP
 ├── Client TCP
 ├── Peer Manager
 ├── Leader Manager (Bully)
 ├── Lock Manager
 └── Multicast Manager
```

Cada nó executa simultaneamente:

- interface interativa (CLI)
- servidor TCP
- protocolos distribuídos
- gerenciamento de peers

---

# Estrutura do Projeto

```
shell/
    prompt.py
    parser.py

network/
    node.py
    server.py
    client.py
    peer_manager.py
    message.py

distributed/
    leader_manager.py
    lock_manager.py
    multicast_manager.py

managers/
    file_manager.py
    process_manager.py

main.py
```

---

# Como executar

Abra um terminal para cada nó.

Exemplo com três nós:

```bash
python3 main.py --id 1 --port 5001

python3 main.py --id 2 --port 5002

python3 main.py --id 3 --port 5003
```

Inicialmente cada nó acredita ser líder.

Conecte os nós:

```
connect 2 127.0.0.1 5002

connect 3 127.0.0.1 5003
```

Após a conexão, ocorre automaticamente uma eleição.

O nó de maior ID torna-se o líder.

O prompt passa a indicar:

```
shell-node3(LIDER)>
```

---

# Protocolos Distribuídos

## Eleição de Líder

Implementação do algoritmo **Bully (Valentão)**.

Características:

- maior ID vence
- reeleição automática após novos peers
- nova eleição quando o líder falha

Mensagens utilizadas:

- ELECTION
- OK
- COORDINATOR

Complexidade:

- Melhor caso: O(N)
- Pior caso: O(N²)

---

## Exclusão Mútua

Existe um coordenador (líder) responsável por controlar os locks.

Cada recurso possui apenas um dono por vez.

Fluxo:

```
Node
    │
LOCK REQUEST
    │
    ▼
Leader
    │
Lock livre?
 ├── Sim → concede
 └── Não → Resource busy
```

Utilizado pelos comandos:

- lock-resource
- unlock-resource
- mkdir

---

## Multicast Confiável

Comandos de escrita são enviados para todos os peers.

Cada mensagem possui:

- sequência
- remetente

Mensagens fora de ordem ficam armazenadas em uma Hold-back Queue até poderem ser entregues.

Garantias:

- entrega FIFO
- nenhuma duplicação
- mesma ordem para todos os nós

---

# Comandos

| Comando | Descrição |
|----------|-----------|
| connect | conecta um novo peer |
| peers | lista peers conhecidos |
| elect | força nova eleição |
| lock-resource | solicita lock distribuído |
| unlock-resource | libera lock |
| mkdir | cria diretório usando exclusão mútua |
| echo | grava arquivo e replica via multicast |
| rmdir | remove diretório |
| rmdir -rf | remove recursivamente |
| cd | muda diretório |
| cp | copia arquivo |
| backup-dir | cria backup usando thread |
| process-test | demonstra fork() |
| thread-test | demonstra threads |
| ls | executa comando externo |
| crash | simula queda do nó |
| revive | recupera nó |
| time | mede tempo de execução |
| exit | encerra a shell |

---

# Testes

## 1. Comandos Locais

```
mkdir teste

cd teste

echo "ola" > a.txt

cp a.txt b.txt

ls -la

process-test

thread-test

backup-dir /tmp

time ls -la
```

---

## 2. Exclusão Mútua

No líder:

```
lock-resource dados
```

Resultado:

```
Lock acquired
```

Em outro nó:

```
lock-resource dados
```

Resultado:

```
Resource busy.
```

Depois:

```
unlock-resource dados
```

O segundo nó consegue adquirir o lock.

---

## 3. Eleição

```
connect 2 127.0.0.1 5002

connect 3 127.0.0.1 5003
```

Ou manualmente:

```
elect
```

Observe as mensagens:

```
ELECTION

OK

COORDINATOR
```

O maior ID torna-se líder.

---

## 4. Tolerância a Falhas

No líder:

```
crash
```

Em outro nó:

```
elect
```

Um novo líder será eleito.

Para recuperar:

```
revive
```

---

## 5. Multicast

Execute:

```
echo "conteudo replicado" > arq.txt
```

Todos os nós conectados receberão:

```
arq.txt
```

com exatamente o mesmo conteúdo.

---

# Comunicação

Todas as mensagens trafegam em JSON.

Exemplo:

```json
{
  "type": "LOCK_REQUEST",
  "sender": 2,
  "payload": {
    "resource": "dados"
  }
}
```

---

# Execução Remota

Também é possível enviar comandos diretamente pela API TCP.

Exemplo:

```python
from network.client import Client
from network.message import Message
from distributed.message_types import MessageType

msg = Message(
    MessageType.COMMAND,
    sender=99,
    payload="peers"
)

print(Client.send_to(
    "127.0.0.1",
    5001,
    msg
).payload)
```

---

# Tecnologias

- Python 3
- TCP Sockets
- Threads
- JSON
- POSIX (Linux/WSL)

---

# Limitações

- `fork()` e `execvp()` exigem Linux ou WSL.
- `ls` depende de ambiente POSIX.
- O algoritmo Bully possui custo O(N²) no pior caso.
- A replicação depende da conectividade entre os peers.

---

# Resumo

O projeto implementa uma MiniShell distribuída composta por múltiplos nós cooperativos capazes de:

- comunicação TCP;
- descoberta de peers;
- eleição automática de líder;
- exclusão mútua distribuída;
- multicast confiável;
- execução de comandos locais e distribuídos;
- simulação de falhas;
- recuperação do sistema.

A combinação desses mecanismos demonstra os principais conceitos de Sistemas Distribuídos em uma aplicação prática e modular.