# ExampleRabbitMQPConsumer

Projeto de estudo do RabbitMQ  
  
## 📈 Dependências e Versões Necessárias  
- **RabbitMQ.Client** 7.0.0  
- **Newtonsoft.Json** 13.0.3  
  
## 🚀 Como Rodar o Projeto  
  
### 1️⃣ Acessar o PowerShell como Administrador  
Abra o **PowerShell** como **Administrador** e **navegue até o diretório de trabalho `sbin` do RabbitMQ**:  
  
```powershell
cd "C:\Program Files\RabbitMQ Server\rabbitmq_server-*\sbin"
```

### 2️⃣ Ativar o Plugin de Gerenciamento  
O **RabbitMQ Management Plugin** permite acessar o **Dashboard via navegador** e visualizar filas, mensagens, exchanges e conexões:  
  
```powershell
.\rabbitmq-plugins.bat enable rabbitmq_management
```

---

### 3️⃣ Iniciar o Serviço do RabbitMQ  
Para iniciar o **RabbitMQ no Windows**, execute:  

```powershell
net start RabbitMQ
```

👉 Isso **inicia o servidor** e permite o processamento de mensagens.

👉 Agora, você pode acessar o **RabbitMQ Dashboard** pelo navegador:  
🔗 [**http://localhost:15672**](http://localhost:15672)  

---

### 4️⃣ Verificar o Status do RabbitMQ  
Para garantir que o **RabbitMQ está rodando corretamente**, use:  
  
```powershell
.\rabbitmqctl.bat status
```

👉 Se estiver tudo certo, a saída mostrará o estado do servidor.

---

### 5️⃣ Parar o Serviço do RabbitMQ  
Caso precise **parar o RabbitMQ**, utilize:  

```powershell
net stop RabbitMQ
```

👉 O serviço será encerrado e o processamento de mensagens será suspenso.

---

## 🎯 Resumo de Comandos  

| **Comando** | **Descrição** |
|------------|--------------|
| `cd "C:\Program Files\RabbitMQ Server\rabbitmq_server-*\sbin"` | Acessa o diretório do RabbitMQ. |
| ` .\rabbitmq-plugins.bat enable rabbitmq_management` | Ativa o **RabbitMQ Management Plugin**. |
| `net start RabbitMQ` | Inicia o servidor RabbitMQ. |
| `.\rabbitmqctl.bat status` | Mostra o status do RabbitMQ. |
| `net stop RabbitMQ` | Para o servidor RabbitMQ. |

