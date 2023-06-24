import pickle
import rpyc
from servico import Content
import threading
from queue import Queue

class Publisher:
    def __init__(self, server, user_id):
        self.server = server
        self.user_id = user_id

    def publish(self, topic, content):
        return self.server.exposed_publish(self.user_id, topic, content)

    def notification_callback(self, contents):
        # Metodo não vai ser utilizado na classe Publisher
        pass


class Subscriber(threading.Thread):
    def __init__(self, server, user_id):
        threading.Thread.__init__(self)
        self.server = server
        self.user_id = user_id
        self.ads_queue = Queue()

    def run(self):
        # Não é necessário login pois já é feito pelo client
        pass

    def subscribe_to(self, topic):
        return self.server.exposed_subscribe_to(self.user_id, topic)

    def unsubscribe_to(self, topic):
        return self.server.exposed_unsubscribe_to(self.user_id, topic)

    def list_topics(self):
        return self.server.exposed_list_topics()

    def show_ads(self):
        if self.ads_queue.empty() == False:
            while self.ads_queue.empty() == False:
                ad = self.ads_queue.get()
                print(f"Notificação recebida: Tópico='{ad.topic}', Autor='{ad.author}', Conteúdo='{ad.data}'")
        else:
            print("Não há novas notificações.")

    def notification_callback(self, contents):
        for content in contents:
            deserialized_content = pickle.loads(content)
            if isinstance(deserialized_content, Content):
                self.ads_queue.put(deserialized_content)
            else:
                print("Content object inválido")


class Client:
    def __init__(self, server_address, server_port):
        self.server_address = server_address
        self.server_port = server_port
        self.user_id = None
        self.publisher = None
        self.subscriber = None
        self.connection = None

    def login(self):
        self.user_id = input("Digite seu ID de usuário: ")

        self.connection = rpyc.connect(self.server_address, self.server_port)
        self.publisher = Publisher(self.connection.root, self.user_id)
        self.subscriber = Subscriber(self.connection.root, self.user_id)
        self.subscriber.start()

        result = self.connection.root.exposed_login(self.user_id, self.subscriber.notification_callback)
        if result:
            print("Login realizado com sucesso.")
            return True
        else:
            print("Não foi feito o login.")
            return False

    def publish(self, topic, content):
        if self.publisher:
            result = self.publisher.publish(topic, content)
            if result:
                print("Anuncio publicado com sucesso.")
            else:
                print("Falhou na publicação do anuncio.")
        else:
            print("Não foi feito o login.")

    def subscribe_to(self, topic):
        if self.subscriber:
            result = self.subscriber.subscribe_to(topic)
            if result:
                print("Subscreveu no tópico com sucesso.")
            else:
                print("Falhou em se subscrever no tópico.")
        else:
            print("Não foi feito o login.")

    def unsubscribe_to(self, topic):
        if self.subscriber:
            result = self.subscriber.unsubscribe_to(topic)
            if result:
                print("Desinscreveu do tópico com sucesso.")
            else:
                print("Falhou em se desinscrever do tópico.")
        else:
            print("Não foi feito o login.")

    def list_topics(self):
        if self.subscriber:
            topics = self.subscriber.list_topics()
            print("Tópicos disponíveis:", topics)
        else:
            print("Não foi feito o login.")

    def show_ads(self):
        if self.subscriber:
            self.subscriber.show_ads()
        else:
            print("Não foi feito o login.")

    def main_menu(self):
        while True:
            print("\nMenu:")
            print("1. Publicar um anuncio")
            print("2. Subscrever em um tópico")
            print("3. Desinscrever de um tópico")
            print("4. Listar tópicos")
            print("5. Mostrar anuncios")
            print("6. Sair")

            choice = input("Digite sua escolha (1-6): ")

            if choice == "1":
                topic = input("Digite o tópico a ser publicado: ")
                content = input("Digite o conteúdo: ")
                self.publish(topic, content)
            elif choice == "2":
                topic = input("Digite o tópico para se subscrever: ")
                self.subscribe_to(topic)
            elif choice == "3":
                topic = input("Digite o tópico para de desinscrever: ")
                self.unsubscribe_to(topic)
            elif choice == "4":
                self.list_topics()
            elif choice == "5":
                self.show_ads()
            elif choice == "6":
                break
            else:
                print("Escolha inválida. Tente novamente.")

        if self.subscriber:
            self.subscriber.ads_queue.put("FIM")
            self.subscriber.join()

        print("Encerrando...")

def main():
    server_address = "localhost"
    server_port = 12345

    client = Client(server_address, server_port)
    if client.login():
        client.main_menu()

if __name__ == "__main__":
    main()