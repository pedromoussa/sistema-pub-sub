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
        # This method will not be used in the Publisher class
        pass


class Subscriber(threading.Thread):
    def __init__(self, server, user_id):
        threading.Thread.__init__(self)
        self.server = server
        self.user_id = user_id
        self.ads_queue = Queue()

    def run(self):
        # No need to log in here since it's already done by the client
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
                print(f"Received notification: Topic='{ad.topic}', Author='{ad.author}', Data='{ad.data}'")
        else:
            print("No new notifications")

    def notification_callback(self, contents):
        for content in contents:
            deserialized_content = pickle.loads(content)
            if isinstance(deserialized_content, Content):
                self.ads_queue.put(deserialized_content)
            else:
                print("Invalid content object")


class Client:
    def __init__(self, server_address, server_port):
        self.server_address = server_address
        self.server_port = server_port
        self.user_id = None
        self.publisher = None
        self.subscriber = None
        self.connection = None

    def login(self):
        self.user_id = input("Enter your user ID: ")

        self.connection = rpyc.connect(self.server_address, self.server_port)
        self.publisher = Publisher(self.connection.root, self.user_id)
        self.subscriber = Subscriber(self.connection.root, self.user_id)
        self.subscriber.start()

        result = self.connection.root.exposed_login(self.user_id, self.subscriber.notification_callback)
        if result:
            print("Login successful.")
            return True
        else:
            print("Login failed.")
            return False

    def publish(self, topic, content):
        if self.publisher:
            result = self.publisher.publish(topic, content)
            if result:
                print("Ad published successfully.")
            else:
                print("Failed to publish ad.")
        else:
            print("Not logged in.")

    def subscribe_to(self, topic):
        if self.subscriber:
            result = self.subscriber.subscribe_to(topic)
            if result:
                print("Subscribed to topic successfully.")
            else:
                print("Failed to subscribe to topic.")
        else:
            print("Not logged in.")

    def unsubscribe_to(self, topic):
        if self.subscriber:
            result = self.subscriber.unsubscribe_to(topic)
            if result:
                print("Unsubscribed from topic successfully.")
            else:
                print("Failed to unsubscribe from topic.")
        else:
            print("Not logged in.")

    def list_topics(self):
        if self.subscriber:
            topics = self.subscriber.list_topics()
            print("Available topics:", topics)
        else:
            print("Not logged in.")

    def show_ads(self):
        if self.subscriber:
            self.subscriber.show_ads()
        else:
            print("Not logged in.")

    def main_menu(self):
        while True:
            print("\nMenu:")
            print("1. Publish an ad")
            print("2. Subscribe to a topic")
            print("3. Unsubscribe from a topic")
            print("4. List topics")
            print("5. Show ads")
            print("6. Exit")

            choice = input("Enter your choice (1-6): ")

            if choice == "1":
                topic = input("Enter the topic to publish: ")
                content = input("Enter the content: ")
                self.publish(topic, content)
            elif choice == "2":
                topic = input("Enter the topic to subscribe: ")
                self.subscribe_to(topic)
            elif choice == "3":
                topic = input("Enter the topic to unsubscribe: ")
                self.unsubscribe_to(topic)
            elif choice == "4":
                self.list_topics()
            elif choice == "5":
                self.show_ads()
            elif choice == "6":
                break
            else:
                print("Invalid choice. Please try again.")

        # Signal the end of processing ads
        if self.subscriber:
            self.subscriber.ads_queue.put("END")
            self.subscriber.join()

        print("Exiting...")

def main():
    server_address = "localhost"  # Update with the actual server address
    server_port = 12345  # Update with the actual server port

    client = Client(server_address, server_port)
    if client.login():
        client.main_menu()

if __name__ == "__main__":
    main()