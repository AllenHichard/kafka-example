from kafka import KafkaProducer

# Configurar o endereço do servidor Kafka
bootstrap_servers = 'localhost:9092'

# Criar um produtor Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Definir o tópico para o qual você deseja enviar mensagens
topic = 'meu-topico'

# Enviar mensagens para o tópico
for i in range(10):
    mensagem = 'Mensagem {}'.format(i)
    print("send", mensagem)
    producer.send(topic, mensagem.encode('utf-8'))

# Fechar a conexão do produtor Kafka
producer.close()
