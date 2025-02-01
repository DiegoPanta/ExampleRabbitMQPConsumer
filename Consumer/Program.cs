using System.Text;
using Consumer;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class PedidoConsumerService
{
    private readonly ConnectionFactory _factory;

    public PedidoConsumerService()
    {
        _factory = new ConnectionFactory
        {
            HostName = "localhost",
            Port = 5672,
            UserName = "guest",
            Password = "guest"
        };
    }

    public async Task ConsumirPedidosAsync()
    {
        await using var connection = await _factory.CreateConnectionAsync(); // Conexão assíncrono
        await using var channel = await connection.CreateChannelAsync(); // Cria um canal assíncrono dentro da conexão com o RabbitMQ

        await channel.QueueDeclareAsync(queue: "fila_pedidos", // Nome da fila no RabbitMQ
                                        durable: true, // A fila será persistente (sobrevive a reinicializações do servidor)
                                        exclusive: false, // Permite que vários consumidores acessem a fila (não exclusiva a uma conexão)
                                        autoDelete: false, // A fila NÃO será deletada automaticamente quando não houver consumidores
                                        arguments: null); // Parâmetros adicionais (ex: TTL, tamanho máximo da fila) - null significa nenhum argumento extra

        //Cria um consumidor assíncrono(AsyncEventingBasicConsumer) que receberá mensagens do RabbitMQ.
        //Está vinculado ao channel, permitindo consumir mensagens de uma fila específica.
        var consumidor = new AsyncEventingBasicConsumer(channel);

        //* ReceivedAsync → Evento acionado quando uma nova mensagem chega na fila.
        //* sender → Referência ao objeto consumidor(AsyncEventingBasicConsumer).
        //* ea(BasicDeliverEventArgs) → Contém dados da mensagem recebida, incluindo:
        //**ea.Body → O conteúdo da mensagem(em bytes).
        //**ea.DeliveryTag → O identificador da mensagem para confirmação(ACK).
        //**ea.RoutingKey → A routing key usada para rotear a mensagem.
        consumidor.ReceivedAsync += async (sender, ea) =>
        {
            var body = ea.Body.ToArray();
            var mensagem = Encoding.UTF8.GetString(body);

            var pedido = JsonConvert.DeserializeObject<Pedido>(mensagem);
            Console.WriteLine($"Pedido recebido: ID={pedido.Id}, Cliente={pedido.Cliente}, Valor={pedido.ValorTotal:C}");

            await Task.Delay(500); //Simula um tempo de processamento 500ms antes de continuar.

            //Confirma todas as mensagens até o DeliveryTag especificado.
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

            //| Código                                              | Função
            //| await channel.BasicAckAsync(ea.DeliveryTag, false); | Confirma somente essa mensagem como processada.
            //| await channel.BasicAckAsync(ea.DeliveryTag, true);  | Confirma essa e todas as mensagens anteriores. Melhora o desempenho ao evitar chamadas múltiplas para o RabbitMQ.
            //| Não chamar BasicAckAsync()                          | A mensagem permanece na fila e pode ser reenviada.
        };

        await channel.BasicConsumeAsync(queue: "fila_pedidos", // Nome da fila no RabbitMQ
                                        autoAck: false, // Define se a confirmação da mensagem é automática ou manual | autoAck: false (Confirmação Manual) | autoAck: true (Confirmação Automática)
                                        consumer: consumidor); // Consumidor responsável pelo processamento
        //autoAck: true => O RabbitMQ remove a mensagem da fila assim que a entrega ao consumidor.
        //autoAck: false => O consumidor precisa chamar BasicAckAsync() para confirmar a mensagem.
        Console.WriteLine("Aguardando pedidos...");
        await Task.Delay(-1); // Mantém o consumidor ativo indefinidamente ou (await Task.Delay(Timeout.Infinite);)
        //await Task.Delay(1000); // Aguarda 1 segundo antes de repetir
    }
}

// Example usage
class Program
{
    static async Task Main()
    {
        var consumerService = new PedidoConsumerService();
        await consumerService.ConsumirPedidosAsync();
    }
}
