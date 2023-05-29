from kafka import KafkaConsumer

# Configurar o endereço do servidor Kafka
bootstrap_servers = 'localhost:9092'

# Criar um consumidor Kafka
consumer = KafkaConsumer('meu-topico', bootstrap_servers=bootstrap_servers)

# Ler e imprimir as mensagens do tópico
for mensagem in consumer:
    print(mensagem.value.decode('utf-8'))

# Fechar a conexão do consumidor Kafka
consumer.close()
