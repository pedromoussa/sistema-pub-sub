import rpyc
import threading
import pickle
from rpyc.utils.server import ThreadedServer
from typing import Callable, List, TypeAlias
from dataclasses import dataclass

UserId: TypeAlias = str

Topic: TypeAlias = str

@dataclass(frozen=True, kw_only=True, slots=True)
class Content:
    author: UserId
    topic: Topic
    data: str

FnNotify: TypeAlias = Callable[[list[Content]], None]

class BrokerGlobals:
    topics = {}
    subscribers = {}
    admin_id = "admin"
    connections = {}

class BrokerService(rpyc.Service):
    def create_topic(self, user_id: UserId, topic_name: str) -> Topic:
        if user_id != BrokerGlobals.admin_id:
            return "Only the admin can create topics."
        if topic_name in BrokerGlobals.topics:
            return "Topic already exists."
        BrokerGlobals.topics[topic_name] = []
        return f"Topic '{topic_name}' created successfully."

    def exposed_login(self, user_id: UserId) -> bool:
        if user_id in BrokerGlobals.subscribers:
            return False
        BrokerGlobals.subscribers[user_id] = []
        BrokerGlobals.connections[user_id] = self._conn
        return True

    def exposed_list_topics(self) -> List[Topic]:
        return list(BrokerGlobals.topics.keys())

    def exposed_publish(self, user_id: UserId, topic: Topic, data: str) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        content = Content(author=user_id, topic=topic, data=data)
        serialized_content = pickle.dumps(content)
        BrokerGlobals.topics[topic].append(serialized_content)
        self._notify_subscribers(topic, [serialized_content])
        return True

    def exposed_subscribe_to(self, user_id: UserId, topic: Topic, callback: FnNotify) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        BrokerGlobals.subscribers[user_id].append((topic, callback))
        self._send_previous_ads(user_id, topic, callback)
        return True

    def exposed_unsubscribe_to(self, user_id: UserId, topic: Topic) -> bool:
        if topic not in BrokerGlobals.topics:
            return False
        BrokerGlobals.subscribers[user_id] = [(t, cb) for t, cb in BrokerGlobals.subscribers[user_id] if t != topic]
        return True

    def on_connect(self, conn):
        self._conn = conn

    def on_disconnect(self, conn):
        user_id = next((user_id for user_id, c in BrokerGlobals.connections.items() if c == conn), None)
        if user_id:
            del BrokerGlobals.connections[user_id]

    def _notify_subscribers(self, topic: Topic, contents: List[Content]) -> None:
        for user_id, subscriptions in BrokerGlobals.subscribers.items():
            conn = BrokerGlobals.connections.get(user_id)
            if conn:
                for t, callback in subscriptions:
                    if t == topic:
                        callback(contents)

    def _send_previous_ads(self, user_id: UserId, topic: Topic, callback: FnNotify) -> None:
        if topic in BrokerGlobals.topics:
            # print(f"Sending previous ads for topic '{topic}' to user '{user_id}'")
            # contents = BrokerGlobals.topics[topic]
            # print(f"Contents: {contents}")
            callback(BrokerGlobals.topics[topic])
        # else:
        #     print(f"No previous ads found for topic '{topic}'")

    @staticmethod
    def start_console_input():
        broker_service = BrokerService()
        while True:
            user_input = input("Enter 'create_topic' command followed by the topic to be created: ")
            if user_input.strip() == "exit":
                break
            broker_service.handle_console_input(user_input)

    def handle_console_input(self, user_input):
        command, *args = user_input.split()
        if command == "create_topic":
            self.create_topic(BrokerGlobals.admin_id, *args)

if __name__ == '__main__':
    server = ThreadedServer(BrokerService, port=12345)
    server_thread = threading.Thread(target=server.start)
    # Start the server thread
    server_thread.start()

    # Start the console input loop
    threading.Thread(target=BrokerService.start_console_input).start()

    # Wait for the server thread to finish (optional)
    server_thread.join()