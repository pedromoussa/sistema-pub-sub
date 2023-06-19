import rpyc

class Client:
    def __init__(self, server_address, server_port, user_id):
        self.client = rpyc.connect(server_address, server_port).root
        self.user_id = user_id

    def login(self):
        result = self.client.exposed_login(self.user_id)
        if result:
            print("Login successful.")
        else:
            print("Login failed.")

    def publish(self):
        topic = input("Enter the topic to publish: ")
        content = input("Enter the content: ")
        result = self.client.exposed_publish(self.user_id, topic, content)
        if result:
            print("Ad published successfully.")
        else:
            print("Failed to publish ad.")

    def subscribe(self):
        topic = input("Enter the topic to subscribe: ")
        result = self.client.exposed_subscribe_to(self.user_id, topic, self.notification_callback)
        if result:
            print("Subscribed to topic successfully.")
        else:
            print("Failed to subscribe to topic.")

    def unsubscribe(self):
        topic = input("Enter the topic to unsubscribe: ")
        result = self.client.exposed_unsubscribe_to(self.user_id, topic)
        if result:
            print("Unsubscribed from topic successfully.")
        else:
            print("Failed to unsubscribe from topic.")

    def list_topics(self):
        topics = self.client.exposed_list_topics()
        print("Available topics:", topics)

    def notification_callback(self, contents):
        for content in contents:
            print(f"Received notification: Topic='{content.topic}', Author='{content.author}', Data='{content.data}'")

def main():
    server_address = "localhost"  # Update with the actual server address
    server_port = 12345  # Update with the actual server port
    user_id = input("Enter your user ID: ")

    client = Client(server_address, server_port, user_id)

    # Login
    client.login()

    while True:
        print("\nMenu:")
        print("1. Publish an ad")
        print("2. Subscribe to a topic")
        print("3. Unsubscribe from a topic")
        print("4. List topics")
        print("5. Exit")

        choice = input("Enter your choice (1-5): ")

        if choice == "1":
            client.publish()
        elif choice == "2":
            client.subscribe()
        elif choice == "3":
            client.unsubscribe()
        elif choice == "4":
            client.list_topics()
        elif choice == "5":
            break
        else:
            print("Invalid choice. Please try again.")

    print("Exiting...")

if __name__ == "__main__":
    main()