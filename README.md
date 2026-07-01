# Shell Distribuída — Guia de Execução e Dicionário de Comandos

Disciplina: Computação Distribuída e Paralela
Professor: Ronaldo Oikawa
Autor: Kauan dos Santos Loche

---

## 1. Arquitetura em um parágrafo

Cada instância (`main.py --id X --port Y`) sobe um **nó** completo: um servidor TCP
(`network/server.py`) escutando em background, uma tabela de peers
(`network/peer_manager.py`) e três gerentes distribuídos — lock
(`distributed/lock_manager.py`), eleição/Bully (`distributed/leader_manager.py`) e
multicast confiável (`distributed/multicast_manager.py`). O usuário interage pela
`MiniShell` (`shell/prompt.py`), que repassa cada linha digitada ao
`CommandParser` (`shell/parser.py`).

---

## 2. Como executar

Abra **um terminal por nó** (mínimo 2, recomendado 3):

```bash
python3 main.py --id 1 --port 5001
python3 main.py --id 2 --port 5002
python3 main.py --id 3 --port 5003
```

Cada nó nasce como líder de si mesmo. Conecte-os para formar o grupo (isso já dispara
uma eleição automaticamente):

```
shell-node1(LIDER)> connect 2 127.0.0.1 5002
shell-node1(LIDER)> connect 3 127.0.0.1 5003
```

Depois de conectados, o prompt do nó de **maior ID** passa a exibir `(LIDER)` — ele é o
coordenador atual (mutex e eleição partem dele).

Para sair de um nó: `exit`.

---

## 3. Dicionário de comandos

| Comando | Sintaxe | Tipo | O que faz |
|---|---|---|---|
| `connect` | `connect <id> <host> <porta>` | **Distribuído** | Registra um peer, troca `PEER_JOIN` e dispara nova eleição (Bully) |
| `peers` | `peers` | Local | Lista os peers conhecidos por este nó (não consulta a rede) |
| `elect` | `elect` | **Distribuído** | Força manualmente uma nova eleição (`ELECTION`/`OK`/`COORDINATOR`) |
| `lock-resource` | `lock-resource <recurso>` | **Distribuído** | Pede ao líder um lock exclusivo sobre `<recurso>` |
| `unlock-resource` | `unlock-resource <recurso>` | **Distribuído** | Libera o lock de `<recurso>` junto ao líder |
| `mkdir` | `mkdir <dir>` | **Distribuído + local** | Pede lock sobre `<dir>`, cria o diretório localmente e libera o lock |
| `echo` | `echo "texto" > arquivo` | **Distribuído (multicast)** | Escreve o arquivo localmente e propaga via multicast ordenado para todos os peers |
| `rmdir` | `rmdir <dir>` | Local | Remove diretório vazio (apenas no nó atual) |
| `rmdir -rf` | `rmdir -rf <dir>` | Local | Remove diretório recursivamente (apenas no nó atual) |
| `cd` | `cd <dir>` | Local | Muda o diretório de trabalho do processo do nó |
| `cp` | `cp <origem> <destino>` | Local | Copia arquivo (apenas no nó atual) |
| `backup-dir` | `backup-dir <dir>` | Local | Faz backup de `<dir>` em `<dir>_backup` usando uma thread separada |
| `process-test` | `process-test` | Local | Demonstra `os.fork()`/`os.wait()` |
| `thread-test` | `thread-test` | Local | Demonstra criação/join de thread |
| `ls ...` | `ls -la` etc. | Local | Executa comando externo via `fork`+`execvp` |
| `crash` | `crash` | Local (efeito remoto) | Simula queda: nó para de responder a qualquer mensagem de rede |
| `revive` | `revive` | Local | Tira o nó do estado de "crash", volta a responder |
| `time` | `time <comando>` | Modificador | Mede o tempo de execução de qualquer comando acima |
| `exit` | `exit` | Local (shell) | Encerra a MiniShell deste nó |

> **Distribuído** = envolve troca de mensagens TCP com outro nó (leader ou peers).
> **Local** = executado inteiramente no processo do nó atual, sem rede.

---

## 4. Como testar cada comando

### 4.1 Comandos locais (testar em um único nó, sem `connect`)

```
shell-node1(LIDER)> mkdir teste          # sem peers, lock é concedido automaticamente
shell-node1(LIDER)> cd teste
shell-node1(LIDER)> echo "ola" > a.txt   # sem peers, só grava local
shell-node1(LIDER)> cp a.txt b.txt
shell-node1(LIDER)> rmdir -rf ../teste
shell-node1(LIDER)> ls -la
shell-node1(LIDER)> process-test
shell-node1(LIDER)> thread-test
shell-node1(LIDER)> backup-dir /tmp
shell-node1(LIDER)> time ls -la
```

### 4.2 Exclusão mútua distribuída (precisa de 2+ nós conectados)

```
# terminal do node3 (líder, maior ID)
shell-node3(LIDER)> lock-resource dados
Lock acquired

# terminal do node2
shell-node2> lock-resource dados
Resource busy.                          # confirma exclusão mútua

# volta ao node3
shell-node3(LIDER)> unlock-resource dados
Lock released

# node2 tenta de novo
shell-node2> lock-resource dados
Lock acquired                           # agora consegue
```

`mkdir` usa esse mesmo mecanismo internamente — teste criando o mesmo diretório em dois
nós "ao mesmo tempo" (peça lock manualmente em um deles antes) para ver o segundo
receber `"Resource busy."`.

### 4.3 Eleição de líder — Algoritmo Valentão (precisa de 2+ nós)

```
shell-node1> connect 2 127.0.0.1 5002
```

Observe nos logs de ambos os terminais as mensagens `ELECTION` → `OK` → `COORDINATOR`.
O nó de maior ID deve terminar com `is_leader = True` e prompt `(LIDER)`. Repita com um
terceiro nó (`connect 3 ...`) e confirme que o nó 3 assume a liderança.

Também é possível forçar manualmente: `elect` em qualquer nó.

### 4.4 Tolerância a falhas (crash do líder)

```
# derruba o líder atual (ex: node3)
shell-node3(LIDER)> crash

# em outro nó, force a checagem de líder (ou aguarde ~5s de heartbeat automático)
shell-node2> elect
```

Verifique com `peers` e observando o prompt que um novo líder foi eleito entre os nós
ainda vivos. O node3 continua de pé mas não responde a nada (simula queda real).
Para trazê-lo de volta: no terminal do node3, digite `revive`.

### 4.5 Multicast confiável e ordenado (precisa de 2+ nós)

```
shell-node1> echo "conteudo replicado" > arq.txt
```

Verifique que `arq.txt` foi criado **em todos os nós conectados**, com o mesmo
conteúdo, no diretório de trabalho de cada um. Os logs de cada nó mostram
`"delivered multicast command -> ..."`, confirmando a entrega ordenada (com número de
sequência por remetente e hold-back queue para mensagens fora de ordem).

### 4.6 Comandos remotos via `COMMAND` (opcional, fora do shell interativo)

O protocolo também aceita um `Message(MessageType.COMMAND, ...)` vindo de qualquer
cliente TCP externo — o servidor repassa para `node.parser.execute(...)` e devolve o
resultado como `RESPONSE`. Isso é usado internamente pelo `Client.send_to`, mas pode ser
testado manualmente com um script Python simples usando `network/client.py`:

```python
from network.client import Client
from network.message import Message
from distributed.message_types import MessageType

msg = Message(MessageType.COMMAND, sender=99, payload="peers")
print(Client.send_to("127.0.0.1", 5001, msg).payload)
```

---

## 5. Resumo do que foi corrigido/implementado

- Corrigidos erros de sintaxe (`message_types.py`, `server.py`) e de instanciação
  (`Client()` sem host/porta, `get_leader()` retornando tupla em vez de dict).
- Implementados do zero: `distributed/leader_manager.py` (Bully), `distributed/
  multicast_manager.py` + `distributed/holdback_queue.py` (multicast ordenado),
  `network/peer_manager.py` (peers), heartbeat automático em `network/node.py` e os
  comandos `connect`, `peers`, `lock-resource`, `unlock-resource`, `elect`, `crash`,
  `revive`.
- Corrigida uma condição de corrida (`RuntimeError: dictionary changed size during
  iteration`) ao iterar peers durante uma eleição concorrente com `PEER_JOIN`.